use super::{
    Backend,
    queries::{self, read_grants as q},
};
use crate::{
    cluster::NodeState,
    content::codec::Key,
    model::{Error, node_read as read, node_refresh, source::SourceOperation},
};
use anyhow::{Result, ensure};
use std::collections::HashSet;
use uuid::Uuid;

pub(super) async fn refresh(
    b: &Backend,
    state: &NodeState,
    key: &Key,
    input: node_refresh::Request,
) -> Result<node_refresh::Reply> {
    ensure!(
        !input.reads.is_empty()
            && input.reads.len() <= node_refresh::LIMIT
            && input.cached.len() <= read::MAX_CACHED,
        Error::NodeReadLimit
    );
    ensure!(
        input.reads.iter().all(|q| q.access.is_none()
            && !q.protection
            && !q.mapping
            && !q.head
            && q.headers.is_empty()
            && q.overrides.is_empty()),
        Error::NodeAuthentication
    );
    let mut tx = q::begin(b).await?;
    queries::node_requests::admit(b, &mut tx, state, key, input.run).await?;
    let mut reply = node_refresh::Reply {
        reads: Vec::new(),
        materials: Vec::new(),
    };
    let mut seen = HashSet::new();
    let mut bytes = 0usize;
    for mut item in input.reads {
        item.cached = input.cached.clone();
        let mut result = super::node_reads::open_in(&mut tx, state, key, item, true).await?;
        for material in std::mem::take(&mut result.materials) {
            if seen.insert(material.version.clone()) {
                bytes += material.json.as_ref().map_or(0, Vec::len)
                    + material.version.id.len() * 2
                    + 128;
                reply.materials.push(material);
            }
        }
        bytes += 4096;
        ensure!(bytes <= read::TARGET_BYTES, Error::NodeReadLimit);
        reply.reads.push(result);
    }
    q::validate_run(&mut tx, state, input.run).await?;
    tx.commit().await?;
    Ok(reply)
}

pub(super) fn refresh_status(_b: &Backend) -> serde_json::Value {
    serde_json::json!({"batch_limit":node_refresh::LIMIT})
}

pub(super) async fn report(
    b: &Backend,
    state: &NodeState,
    key: &Key,
    input: read::Report,
) -> Result<Vec<Uuid>> {
    ensure!(
        !input.segments.is_empty() && input.segments.len() <= 16,
        s3s::s3_error!(InvalidArgument)
    );
    let _serial = b.read_reports.lock().await;
    let mut tx = q::begin(b).await?;
    q::access(b, &mut tx, state, key, input.run).await?;
    q::node_read_owned(state, input.run, input.read)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?;
    let mapping = q::report_window(input.read, input.window)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeExpired)?
        .0;
    let scopes = mapping.report_scopes(&input.segments)?;
    let mut count = q::verification_count().fetch_one(&mut *tx).await?;
    let mut tasks = Vec::new();
    for (identity, scope) in scopes {
        let mut hash = blake3::Hasher::new_derive_key("mokyu node read report v1");
        hash.update(input.window.as_bytes());
        hash.update(identity.as_bytes());
        let task = Uuid::from_bytes(hash.finalize().as_bytes()[..16].try_into().unwrap());
        if !q::reported(task).fetch_one(&mut *tx).await? {
            let snapshot = queries::source_snapshot(&mut tx, &scope, true).await?;
            if snapshot.state != "ready" || snapshot.quarantine_code.is_some() {
                continue;
            }
            ensure!(count < 64, Error::NodeReadBusy);
            let operation = SourceOperation {
                principal: crate::authorization::Principal::System,
                mode: "single_control".into(),
                code: Some("hash_mismatch".into()),
                node: None,
                snapshot,
            };
            let detail = serde_json::json!({"policy":"verify","operation":"verify","storage_id":scope.storage_id,"source":operation.snapshot.summary()?,"source_location":operation.snapshot.location(),"execution_mode":"single_control","source_operation":operation});
            queries::insert_task(task, "integrity", None, detail, None)
                .execute(&mut *tx)
                .await?;
            count += 1;
        }
        tasks.push(task);
    }
    tx.commit().await?;
    Ok(tasks)
}
