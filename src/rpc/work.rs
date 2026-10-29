use super::{read, wire};
use crate::model::node_work as work;
use tonic::Status;

pub(super) fn exchange(input: wire::WorkRequest) -> Result<work::Exchange, Status> {
    let invalid = || Status::invalid_argument("invalid work exchange");
    let size = prost::Message::encoded_len(&input);
    if size > crate::model::node_pack::RESULT_LIMIT {
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
    if size > read::REQUEST_LIMIT
        && !value
            .completion
            .as_ref()
            .is_some_and(|c| matches!(c.outcomes.as_slice(), [work::Outcome::Packed(_)]))
    {
        return Err(invalid());
    }
    if let Some(completion) = &value.completion {
        for outcome in &completion.outcomes {
            if let work::Outcome::ExternalTest(report) = outcome {
                report.validate().map_err(|_| invalid())?;
            }
            if matches!(outcome, work::Outcome::Hashed { hash } if hash.len()!=32) {
                return Err(invalid());
            }
            if let work::Outcome::Packed(result) = outcome {
                if result.chunks.len() > crate::content::pack::MAX_MEMBERS
                    || result.packs.len() > crate::content::pack::MAX_MEMBERS / 2
                    || result.packs.iter().any(|(_, _, digest)| digest.len() != 44)
                {
                    return Err(invalid());
                }
            }
        }
    }
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
        external_connection_json: value
            .external
            .map(|snapshot| serde_json::to_vec(&snapshot))
            .transpose()
            .map_err(|_| Status::internal("external connection encoding failed"))?,
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "work plan exceeds message budget",
        ));
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn large_results_are_reserved_for_one_bounded_pack() {
        let request = |outcomes| wire::WorkRequest {
            run: Some(wire::Run {
                boot_id: Uuid::new_v4().to_string(),
                fence: Uuid::new_v4().to_string(),
            }),
            operation_id: Uuid::now_v7().to_string(),
            completion_json: Some(
                serde_json::to_vec(&work::Completion {
                    operation: Uuid::now_v7(),
                    outcomes,
                })
                .unwrap(),
            ),
            class: 1,
            max_items: 64,
            cached: vec![],
        };
        let outcome = work::Outcome::Packed(crate::model::node_pack::Encoded {
            chunks: vec![u32::MAX; 4096],
            packs: (0..2048).map(|i| (i, u32::MAX, "x".repeat(44))).collect(),
        });
        assert!(exchange(request(vec![work::Outcome::Hashed { hash: vec![0; 31] }])).is_err());
        assert!(exchange(request(vec![work::Outcome::Hashed { hash: vec![0; 32] }])).is_ok());
        let input = request(vec![outcome.clone()]);
        assert!(prost::Message::encoded_len(&input) > read::REQUEST_LIMIT);
        assert!(exchange(input).is_ok());
        assert!(exchange(request(vec![outcome.clone(), work::Outcome::Retry])).is_err());
        let work::Outcome::Packed(mut result) = outcome else {
            unreachable!()
        };
        result.chunks.push(1);
        assert!(exchange(request(vec![work::Outcome::Packed(result)])).is_err());
        assert!(
            exchange(request(vec![work::Outcome::Inspected {
                code: Some("x".repeat(read::REQUEST_LIMIT))
            }]))
            .is_err()
        );
    }
}
