use super::{read, wire};
use crate::model::node_work as work;
use tonic::Status;

pub(super) fn exchange(input: wire::WorkRequest) -> Result<work::Exchange, Status> {
    let invalid = || Status::invalid_argument("invalid work exchange");
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(invalid());
    }
    let value = work::Exchange {
        run: read::run(input.run)?,
        operation: super::uuid(&input.operation_id)?,
        completion: input
            .completion_json
            .map(|value| serde_json::from_slice(&value))
            .transpose()
            .map_err(|_| invalid())?,
        class: input.class,
        max_items: input.max_items as usize,
        cached: read::cached(input.cached)?,
    };
    value.hash().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn reply(value: work::Reply) -> Result<wire::WorkReply, Status> {
    let reply = wire::WorkReply {
        operation_id: value.operation.to_string(),
        plan_json: value
            .plan
            .map(|plan| serde_json::to_vec(&plan))
            .transpose()
            .map_err(|_| Status::internal("work plan encoding failed"))?,
        materials: value.materials.into_iter().map(read::material).collect(),
        grants: value.grants.into_iter().map(read::grant).collect(),
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "work plan exceeds message budget",
        ));
    }
    Ok(reply)
}
