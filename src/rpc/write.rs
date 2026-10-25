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

pub(super) fn prepare(
    input: wire::PrepareChunksRequest,
) -> Result<crate::model::node_chunk::Prepare, Status> {
    let value = crate::model::node_chunk::Prepare {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        batch: input.batch_id.try_into().map_err(|_| invalid())?,
        offset: input.offset.try_into().map_err(|_| invalid())?,
        items: input
            .chunks
            .into_iter()
            .map(|value| {
                Ok(crate::model::node_chunk::Input {
                    hash: value.hash.try_into().map_err(|_| invalid())?,
                    raw_size: value.raw_size.try_into().map_err(|_| invalid())?,
                    payload_hash: value.payload_hash.try_into().map_err(|_| invalid())?,
                    payload_size: value.payload_size.try_into().map_err(|_| invalid())?,
                    compressed: value.compressed,
                })
            })
            .collect::<Result<_, Status>>()?,
        parallelism: if input.parallelism == 0 {
            1
        } else {
            input.parallelism
        },
        cached: read::cached(input.cached)?,
    };
    value.end().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn commit(
    input: wire::CommitChunksRequest,
) -> Result<crate::model::node_chunk::Commit, Status> {
    let mut value = crate::model::node_chunk::Commit {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        batch: input.batch_id.try_into().map_err(|_| invalid())?,
        encoded: input
            .chunks
            .into_iter()
            .map(|value| {
                Ok(crate::model::node_chunk::Encoded {
                    chunk: super::uuid(&value.chunk_id)?,
                    digest: value.digest.try_into().map_err(|_| invalid())?,
                })
            })
            .collect::<Result<_, Status>>()?,
    };
    value.hash().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn publish(input: wire::PublishWriteRequest) -> Result<write::Publish, Status> {
    let value = write::Publish {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        size: input.size.try_into().map_err(|_| invalid())?,
        etag: input.etag,
        checksums: serde_json::from_slice(&input.checksums_json).map_err(|_| invalid())?,
        if_match: input.if_match,
        if_none: input.if_none,
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn prepared(
    value: crate::model::node_chunk::Prepared,
) -> Result<wire::PreparedChunks, Status> {
    let reply = wire::PreparedChunks {
        batch_id: value.batch as u64,
        end: value.end as u64,
        committed: value.committed,
        chunks: value
            .items
            .into_iter()
            .map(|value| {
                Ok(wire::PreparedChunk {
                    offset: value.offset as u64,
                    reused: value.reused,
                    payload_hash: value.payload_hash.to_vec(),
                    chunk_json: serde_json::to_vec(&value.chunk)
                        .map_err(|_| Status::internal("cannot encode chunk plan"))?,
                })
            })
            .collect::<Result<_, Status>>()?,
        materials: value.materials.into_iter().map(read::material).collect(),
        storage: value.storage.map(read::version),
        grants: value.grants.into_iter().map(read::grant).collect(),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "chunk reply exceeds message budget",
        ));
    }
    Ok(reply)
}

pub(super) fn reply(value: write::Reply) -> Result<wire::WriteReply, Status> {
    let reply = wire::WriteReply {
        operation_id: value.operation.to_string(),
        stream_id: value.stream.map_or_else(String::new, |id| id.to_string()),
        phase: match value.phase {
            write::Phase::Active => wire::WritePhase::WriteActive,
            write::Phase::Aborted => wire::WritePhase::WriteAborted,
            write::Phase::Expired => wire::WritePhase::WriteExpired,
            write::Phase::Published => wire::WritePhase::WritePublished,
        } as i32,
        materials: value.materials.into_iter().map(read::material).collect(),
        storage: value.storage.map(read::version),
        grants: value.grants.into_iter().map(read::grant).collect(),
        published: value.published.map(|value| wire::PublishedWrite {
            size: value.size as u64,
            etag: value.etag,
            last_modified: value.modified.to_rfc3339(),
        }),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "write reply exceeds message budget",
        ));
    }
    Ok(reply)
}
