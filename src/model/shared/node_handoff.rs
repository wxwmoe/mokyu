use super::{
    Backend,
    queries::{
        node_handoff as q,
        storage_cluster::{Tx, admin},
    },
};
use crate::{
    authorization::Principal,
    model::{
        Error,
        node::{Handoff, HandoffAction, HandoffInput, NodeDrain, NodeReport},
    },
};
use anyhow::{Result, ensure};
use uuid::Uuid;

pub(super) async fn manage(
    backend: &Backend,
    principal: &Principal,
    node: Uuid,
    input: Option<HandoffInput>,
) -> Result<Handoff> {
    let mut tx = if input.is_some() {
        super::queries::storage_cluster::begin(backend).await?
    } else {
        super::queries::storage_payload::begin(backend).await?
    };
    admin(&mut tx, principal).await?;
    let (role, enabled) = q::role(node)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NodeInactive)?;
    ensure!(
        enabled
            && matches!(
                role.as_str(),
                "control" | "data" | "worker" | "node" | "storage"
            ),
        Error::NodeInactive
    );
    let mut row = q::row(node).fetch_optional(&mut *tx).await?;
    if let Some(input) = input {
        let current = row.as_ref().map_or("serving", |r| r.state.as_str());
        let revision = row.as_ref().map_or(0, |r| r.revision);
        ensure!(revision == input.revision, Error::OperationAborted);
        let next = match (current, input.action) {
            ("serving", HandoffAction::Prepare) => "preparing",
            ("preparing", HandoffAction::Cancel) | ("offline_ready", HandoffAction::Resume) => {
                "resuming"
            }
            ("preparing", HandoffAction::Prepare)
            | ("resuming", HandoffAction::Cancel | HandoffAction::Resume) => current,
            _ => return Err(Error::OperationAborted.into()),
        };
        if next != current {
            if role == "control" && next == "preparing" {
                ensure!(
                    q::other_control(node).fetch_one(&mut *tx).await?,
                    s3s::s3_error!(
                        InvalidRequest,
                        "another ready control is required before this control can go offline"
                    )
                );
            }
            let operation = Uuid::now_v7();
            if row.is_some() {
                ensure!(
                    q::update(node, revision, operation, next)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1,
                    Error::OperationAborted
                );
            } else {
                q::insert(node, operation, next).execute(&mut *tx).await?;
            }
            if next == "preparing" && !matches!(role.as_str(), "control" | "storage") {
                q::tails(node).execute(&mut *tx).await?;
            }
            row = q::row(node).fetch_optional(&mut *tx).await?;
        }
    }
    let (state, operation, revision) = row.map_or_else(
        || ("serving".into(), None, 0),
        |row| (row.state, Some(row.operation), row.revision),
    );
    let (pending, (pending_bytes, damaged_tails)) =
        if state == "preparing" && !matches!(role.as_str(), "control" | "storage") {
            (
                q::counts(&mut tx, node).await?,
                q::details(node).fetch_one(&mut *tx).await?,
            )
        } else {
            (NodeDrain::default(), (0, 0))
        };
    let storage = if role == "storage" && state == "preparing" {
        Some(storage(&mut tx, node).await?)
    } else {
        None
    };
    let view = Handoff {
        storage,
        state,
        operation,
        revision,
        pending,
        pending_bytes,
        damaged_tails,
    };
    tx.commit().await?;
    Ok(view)
}

pub(super) async fn storage(
    tx: &mut Tx<'_>,
    node: Uuid,
) -> Result<crate::model::node::StorageDrain> {
    use crate::model::node::StorageDrain;
    let Some((c, media)) = super::storage_cluster::draining_source(tx, node).await? else {
        return Ok(StorageDrain::default());
    };
    let (fragments, moves, reservations, recovery_heads) =
        super::queries::storage_move::status(media)
            .fetch_one(&mut **tx)
            .await?;
    Ok(StorageDrain {
        groups: c
            .manifest
            .groups
            .iter()
            .filter(|g| {
                g.members.contains(&media)
                    || g.replacing.as_ref().is_some_and(|r| r.target == media)
            })
            .count() as i64,
        fragments,
        moves,
        reservations,
        recovery_heads,
    })
}

pub(super) async fn observed(tx: &mut Tx<'_>, node: Uuid, report: &NodeReport) -> Result<()> {
    let Some(revision) = report.status["handoff_revision"]
        .as_i64()
        .filter(|v| *v > 0)
    else {
        return Ok(());
    };
    if report.status["draining"] != true || report.status["handoff_reconciled"] != true {
        return Ok(());
    }
    let Some(row) = q::row(node).fetch_optional(&mut **tx).await? else {
        return Ok(());
    };
    if row.revision != revision {
        return Ok(());
    }
    let (role, _) = q::role(node).fetch_one(&mut **tx).await?;
    let next = match row.state.as_str() {
        "resuming" => "serving",
        "preparing" if role == "control" => {
            if report.status["control_operations"].as_u64() != Some(0)
                || report.status["active_streams"].as_u64() != Some(0)
            {
                return Ok(());
            }
            "offline_ready"
        }
        "preparing" if role == "storage" => {
            if report.status["storage_admission_closed"] != true
                || report.status["storage_streams"].as_u64() != Some(0)
                || !storage(tx, node).await?.empty()
            {
                return Ok(());
            }
            "offline_ready"
        }
        "preparing" => {
            q::tails(node).execute(&mut **tx).await?;
            if ["s3", "web"]
                .iter()
                .any(|key| report.status["runtime"]["http"][*key]["active"].as_u64() != Some(0))
            {
                return Ok(());
            }
            if !q::counts(tx, node).await?.empty() {
                return Ok(());
            }
            "offline_ready"
        }
        _ => return Ok(()),
    };
    q::advance(node, revision, next).execute(&mut **tx).await?;
    Ok(())
}
