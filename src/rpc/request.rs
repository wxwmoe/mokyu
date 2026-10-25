use super::{read, wire};
use crate::model::{ReadRoute, node_read, node_request};
use tonic::Status;

pub(super) fn prepare(
    input: wire::PrepareRequestInput,
    region: &str,
) -> Result<node_request::Prepare, Status> {
    let invalid = || Status::invalid_argument("invalid request preparation");
    if prost::Message::encoded_len(&input) > read::REQUEST_LIMIT {
        return Err(Status::resource_exhausted(
            "request preparation exceeds message budget",
        ));
    }
    let value = node_request::Prepare {
        run: read::run(input.run)?,
        access: (!input.access_key.is_empty()).then_some(input.access_key),
        route: (!input.bucket.is_empty()).then_some(ReadRoute::Bucket {
            name: input.bucket,
            key: String::new(),
            alias: None,
        }),
        include: input
            .include
            .into_iter()
            .map(|value| match wire::MaterialType::try_from(value) {
                Ok(wire::MaterialType::S3Credential) => Ok(node_read::MaterialType::Credential),
                Ok(wire::MaterialType::Bucket) => Ok(node_read::MaterialType::Bucket),
                Ok(wire::MaterialType::ReadPolicy) => Ok(node_read::MaterialType::Policy),
                _ => Err(invalid()),
            })
            .collect::<Result<_, _>>()?,
        cached: read::cached(input.cached)?,
        query: input
            .query
            .map(|query| -> Result<_, Status> {
                use wire::prepare_request_input::Query;
                Ok(match query {
                    Query::DescribeBucket(_) => node_request::Query::Bucket,
                    Query::ObjectAcl(value) => node_request::Query::Acl { key: value.key },
                    Query::ListBuckets(value) => node_request::Query::Buckets {
                        prefix: value.prefix.unwrap_or_default(),
                        region: value.region,
                        expected_region: region.to_owned(),
                        token: value.token,
                        limit: value.limit.try_into().map_err(|_| invalid())?,
                    },
                    Query::ListObjects(value) => node_request::Query::Objects {
                        prefix: value.prefix.unwrap_or_default(),
                        delimiter: value.delimiter.unwrap_or_default(),
                        after: value.after,
                        token: value.token,
                        limit: value.limit.try_into().map_err(|_| invalid())?,
                    },
                })
            })
            .transpose()?,
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

pub(super) fn reply(input: node_request::Reply) -> Result<wire::RequestReply, Status> {
    let catalog_json = input
        .catalog
        .map(|value| serde_json::to_vec(&value))
        .transpose()
        .map_err(|_| Status::internal("cannot encode catalog reply"))?
        .unwrap_or_default();
    let value = read::reply(input.context);
    let reply = wire::RequestReply {
        outcome: value.outcome,
        materials: value.materials,
        credential: value.credential,
        bucket: value.bucket,
        policy: value.policy,
        catalog_json,
    };
    if prost::Message::encoded_len(&reply) > super::MESSAGE_LIMIT {
        return Err(Status::resource_exhausted(
            "request reply exceeds message budget",
        ));
    }
    Ok(reply)
}
