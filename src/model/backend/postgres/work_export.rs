use super::*;

pub(crate) async fn read(
    pool: &PgPool,
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
    let mut db = pool.begin().await?;
    authenticate_rpc_node(&mut db, node, key, false).await?;
    require_node_run(&mut db, node, input.run, true).await?;
    ensure!(
        matches!(node.role.as_str(), "worker" | "node"),
        Error::NodeInactive
    );
    let row = q::batch(node.node_id, input.operation)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(Error::NodeExpired)?;
    owned(node, input.run, &row)?;
    ensure!(
        row.valid_for_ms > 0 && row.result_hash.is_none(),
        Error::NodeExpired
    );
    let task = Execution {
        id: row.task_id.ok_or(Error::NodeExpired)?,
        token: row.token,
    };
    ensure!(
        q::task(task.id, task.token, node.node_id)
            .fetch_optional(&mut *db)
            .await?
            .is_some(),
        Error::NodeExpired
    );
    let work::Plan::ExternalTransfer(plan) = row.plan.ok_or(Error::NodeOperationConflict)?.0 else {
        return Err(Error::NodeOperationConflict.into());
    };
    ensure!(!plan.options.source_external, Error::NodeOperationConflict);
    let run = super::super::super::replication_transactions::owned(&mut db, task).await?;
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
    let native = super::super::super::replication_remote_transactions::native(
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
    let original = queries::node_read_stream(native.stream)
        .fetch_one(&mut *db)
        .await?;
    let raw = super::super::super::replication_remote_transactions::raw_read(
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
