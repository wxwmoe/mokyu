use super::node_reads::protect_node_read;
use super::{Backend, queries, transactions};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{Error, node_read as read, node_work as work},
};
use anyhow::{Result, ensure};

pub(crate) async fn read(
    backend: &Backend,
    node: &NodeState,
    key: &Key,
    input: work::ReadTransfer,
) -> Result<(
    read::Reply,
    Option<crate::model::replication::external::RawRead>,
)> {
    ensure!(
        input.length > 0 && input.length as usize <= crate::storage::external::objects::WINDOW,
        Error::NodeOperationConflict
    );
    let mut db = super::node_writes::start(backend, node, key, input.run).await?;
    let (offer, task) =
        super::node_work::checkpoint(&mut db, node, input.run, input.operation).await?;
    let work::Plan::ExternalTransfer(plan) = offer else {
        anyhow::bail!(Error::NodeOperationConflict);
    };
    ensure!(
        plan.options.source_external != plan.options.target_external
            && (!plan.options.source_external
                || plan.state.phase == crate::model::replication::external::Phase::Compare),
        Error::NodeOperationConflict
    );
    let run = transactions::replication::owned(&mut db, task).await?;
    ensure!(
        run.phase == "verify"
            && run.generation == plan.generation
            && matches!(
                plan.state.phase,
                crate::model::replication::external::Phase::Compare
                    | crate::model::replication::external::Phase::Transfer
            ),
        Error::NodeOperationConflict
    );
    let native = transactions::replication_remote::native(
        &mut db,
        &run,
        plan.item.ok_or(Error::NodeOperationConflict)?,
    )
    .await?;
    ensure!(
        native.is_some() && native == plan.native,
        Error::SourceUnavailable
    );
    let native = native.unwrap();
    ensure!(
        input.offset <= native.object.size
            && u64::from(input.length) <= native.object.size - input.offset,
        Error::NodeOperationConflict
    );
    let original = queries::read_grants::node_read_stream(native.stream)
        .fetch_one(&mut *db)
        .await?;
    let raw = transactions::replication_remote::raw_read(
        &mut db,
        &run,
        &original,
        input.offset,
        input.length,
    )
    .await?;
    let length = raw.as_ref().map_or(input.length, |raw| raw.length);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::RANGE,
        format!(
            "bytes={}-{}",
            input.offset,
            input.offset + u64::from(length) - 1
        )
        .parse()?,
    );
    let query = read::Query {
        run: input.run,
        operation: input.read,
        route: crate::model::ReadRoute::Bucket {
            name: format!("transfer:{}", input.operation),
            key: original.id.to_string(),
            alias: None,
        },
        access: None,
        head: false,
        web: false,
        mapping: true,
        protection: true,
        policy: false,
        headers,
        overrides: Default::default(),
        cached: input.cached,
        max_items: 256,
        lanes: 1,
        permits: Default::default(),
        max_bytes: read::TARGET_BYTES,
        body: None,
    };
    let mut reply = read::Reply::new(read::Outcome::Ready);
    let object = read::Object::from_stream(&original)?;
    reply.object = Some(reply.material(
        node.deployment_id,
        read::MaterialType::Object,
        object.id.to_string(),
        &object,
        &query.cached,
    )?);
    reply.offset = input.offset;
    reply.length = u64::from(length);
    reply.etag = original.etag.clone();
    let reply = protect_node_read(
        &mut db,
        node,
        key,
        &query,
        &original,
        reply,
        &mut Default::default(),
    )
    .await?;
    ensure!(
        reply.outcome != read::Outcome::Pending,
        Error::NodeSourcePending
    );
    ensure!(
        reply.outcome == read::Outcome::Ready,
        Error::SourceUnavailable
    );
    db.commit().await?;
    Ok((reply, raw))
}
