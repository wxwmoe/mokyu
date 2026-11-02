use super::*;

pub(in super::super) async fn report_node_read(
    backend: &Backend,
    state: &crate::cluster::NodeState,
    key: &crate::content::codec::Key,
    input: crate::model::node_read::Report,
) -> Result<Vec<Uuid>> {
    use crate::model::{Error, source::SourceOperation};
    ensure!(
        !input.segments.is_empty() && input.segments.len() <= 16,
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = backend.pool.begin().await?;
    backend.fence_control(&mut tx).await?;
    authenticate_rpc_node(&mut tx, state, key, false).await?;
    ensure!(
        matches!(state.role.as_str(), "data" | "worker" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, state, input.run, false).await?;
    queries::lock_staging(format!("mokyu:node-read:{}", state.node_id))
        .execute(&mut *tx)
        .await?;
    queries::node_read_owned(state, input.run, input.read)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    let mapping = queries::node_read_report_window(input.read, input.window)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?
        .0;
    let scopes = super::super::read_map::report_scopes(&mapping, &input.segments)?;
    queries::lock_staging("mokyu:read-verification".into())
        .execute(&mut *tx)
        .await?;
    let mut pending = queries::node_read_verifications()
        .fetch_one(&mut *tx)
        .await?;
    let mode = if backend.control_epoch.is_some() {
        "single_control"
    } else {
        "cluster"
    };
    let mut tasks = Vec::with_capacity(scopes.len());
    for (identity, scope) in scopes {
        let mut hash = blake3::Hasher::new_derive_key("mokyu node read report v1");
        hash.update(input.window.as_bytes());
        hash.update(identity.as_bytes());
        let task = Uuid::from_bytes(hash.finalize().as_bytes()[..16].try_into().unwrap());
        if queries::node_read_reported(task)
            .fetch_one(&mut *tx)
            .await?
        {
            tasks.push(task);
            continue;
        }
        let snapshot = queries::source_snapshot(&mut tx, &scope, true).await?;
        if snapshot.state != "ready" || snapshot.quarantine_code.is_some() {
            continue;
        }
        ensure!(pending < 64, Error::NodeReadBusy);
        let operation = SourceOperation {
            principal: crate::authorization::Principal::System,
            mode: mode.into(),
            // The worker replaces this suspicion only after inspecting the exact backend source.
            code: Some("hash_mismatch".into()),
            node: None,
            snapshot,
        };
        let detail = json!({"policy":"verify","operation":"verify","storage_id":scope.storage_id,
            "source":operation.snapshot.summary()?,"source_location":operation.snapshot.location(),
            "execution_mode":mode,"source_operation":operation});
        insert_task(&mut tx, task, "integrity", None, detail, None).await?;
        pending += 1;
        tasks.push(task);
    }
    tx.commit().await?;
    Ok(tasks)
}
