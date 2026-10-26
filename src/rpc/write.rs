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

pub(super) fn prepare_tail(
    input: wire::PrepareTailRequest,
) -> Result<crate::model::node_tail::Prepare, Status> {
    let value = crate::model::node_tail::Prepare {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        offset: input.offset.try_into().map_err(|_| invalid())?,
        size: input.size.try_into().map_err(|_| invalid())?,
        hash: input.hash.try_into().map_err(|_| invalid())?,
        cached: read::cached(input.cached)?,
    };
    value.hash().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn tail(value: crate::model::node_tail::Prepared) -> wire::PreparedTail {
    wire::PreparedTail {
        fragment_id: value.fragment.to_string(),
        object_key: value.object_key,
        end: value.end as u64,
        committed: value.committed,
        materials: value.materials.into_iter().map(read::material).collect(),
        storage: value.storage.map(read::version),
        grants: value.grants.into_iter().map(read::grant).collect(),
    }
}

pub(super) fn prepare_raw(
    input: wire::PrepareRawRequest,
) -> Result<crate::model::node_raw::Prepare, Status> {
    let value = crate::model::node_raw::Input {
        local: input.local,
        size: input.size.try_into().map_err(|_| invalid())?,
        digest: input.digest.try_into().map_err(|_| invalid())?,
        hash: if input.content_hash.is_empty() {
            None
        } else {
            Some(input.content_hash.try_into().map_err(|_| invalid())?)
        },
    };
    value.validate().map_err(|_| invalid())?;
    Ok(crate::model::node_raw::Prepare {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        input: value,
    })
}

pub(super) fn commit_raw(
    input: wire::CommitRawRequest,
) -> Result<crate::model::node_raw::Commit, Status> {
    Ok(crate::model::node_raw::Commit {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        source: super::uuid(&input.source_id)?,
    })
}

pub(super) fn raw(value: crate::model::node_raw::Prepared) -> wire::PreparedRaw {
    wire::PreparedRaw {
        source_id: value.source.to_string(),
        size: value.size as u64,
        committed: value.committed,
        grants: value.grants.into_iter().map(read::grant).collect(),
    }
}

pub(super) fn begin(input: wire::BeginWriteRequest) -> Result<write::Begin, Status> {
    if prost::Message::encoded_len(&input) > write::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "write request exceeds message budget",
        ));
    }
    let intent = write::Intent {
        completion: None,
        access: input.access_key,
        credential_revision: input
            .credential_revision
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
        bucket: input.bucket,
        key: input.key,
        metadata: serde_json::from_slice(&input.metadata_json).map_err(|_| invalid())?,
        public: input.public_read,
        size: input
            .size
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid())?,
        copy_read: input
            .copy_read_id
            .map(|value| super::uuid(&value))
            .transpose()?,
        part: input
            .part
            .map(|part| -> Result<_, Status> {
                Ok(write::Part {
                    upload: super::uuid(&part.upload_id)?,
                    number: part.number,
                    algorithm: part.algorithm,
                })
            })
            .transpose()?,
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

pub(super) fn copy(input: wire::CopyChunksRequest) -> Result<write::CopyBatch, Status> {
    let value = write::CopyBatch {
        run: read::run(input.run)?,
        operation: operation(&input.operation_id)?,
        batch: input.batch_id.try_into().map_err(|_| invalid())?,
        offset: input.offset.try_into().map_err(|_| invalid())?,
    };
    value.hash().map_err(|_| invalid())?;
    Ok(value)
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
        packed: input
            .packed_json
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| invalid()))
            .transpose()?,
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
        packing_json: value
            .packing
            .map(|batch| serde_json::to_vec(&batch))
            .transpose()
            .map_err(|_| Status::internal("cannot encode pack plan"))?,
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
        bucket_id: value.bucket.map_or_else(String::new, |id| id.to_string()),
        stream_id: value.stream.map_or_else(String::new, |id| id.to_string()),
        phase: match value.phase {
            write::Phase::Active => wire::WritePhase::WriteActive,
            write::Phase::Busy => wire::WritePhase::WriteBusy,
            write::Phase::Aborted => wire::WritePhase::WriteAborted,
            write::Phase::Expired => wire::WritePhase::WriteExpired,
            write::Phase::Published => wire::WritePhase::WritePublished,
        } as i32,
        copy_references: value.copy_references,
        checksum_algorithm: value.checksum_algorithm,
        completion_json: value
            .completion
            .map(|value| serde_json::to_vec(&value))
            .transpose()
            .map_err(|_| Status::internal("invalid completion reply"))?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn raw_input_requires_bounded_length_and_exact_digests() {
        let mut input = wire::PrepareRawRequest {
            local: false,
            run: Some(wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            }),
            operation_id: Uuid::now_v7().to_string(),
            size: 0,
            digest: vec![0; 16],
            content_hash: vec![1; 32],
        };
        assert!(prepare_raw(input.clone()).is_ok());
        input.size = 5 * 1024 * 1024 * 1024 + 1;
        assert!(prepare_raw(input.clone()).is_err());
        input.size = 0;
        input.content_hash.pop();
        assert!(prepare_raw(input.clone()).is_err());
        input.content_hash.clear();
        assert!(prepare_raw(input.clone()).is_ok());
        input.digest.pop();
        assert!(prepare_raw(input).is_err());
    }

    #[test]
    fn write_admission_distinguishes_missing_and_zero_credential_revisions() {
        let mut input = wire::BeginWriteRequest {
            run: Some(wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            }),
            operation_id: Uuid::now_v7().to_string(),
            access_key: "writer".into(),
            bucket: "bucket".into(),
            key: "object".into(),
            metadata_json: b"{}".to_vec(),
            credential_revision: None,
            ..Default::default()
        };
        assert!(begin(input.clone()).is_err());
        input.credential_revision = Some(0);
        assert_eq!(begin(input.clone()).unwrap().intent.credential_revision, 0);
        input.credential_revision = Some(u64::MAX);
        assert!(begin(input).is_err());
    }
}
