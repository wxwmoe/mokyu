use super::{
    redundancy_queries, redundancy_transactions, replica_queries as q,
    replication_queries as transfer, transactions,
};
use crate::{
    authorization::Principal,
    model::{
        Error, Id, StreamCreate, StreamPurpose,
        replication::{Conflict, Operation, Options, Scope},
    },
};
use anyhow::{Result, ensure};
use q::Tx;
use serde_json::{Value, json};
use sqlx::{Acquire, types::Json};
use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub(super) struct Target {
    pub link_id: Uuid,
    pub link_revision: i64,
    pub target_bucket: Uuid,
    pub target_key: String,
    pub storage_id: Uuid,
    pub profile_revision: i64,
    pub authority: Json<Principal>,
    pub permissions: String,
    pub expected_version: Option<Id>,
    pub candidate: Option<Id>,
    pub task_id: Option<Uuid>,
}

impl Target {
    fn options(&self, source: Uuid, key: &str) -> Result<Options> {
        let options = Options {
            source_bucket: source,
            target_bucket: self.target_bucket,
            source_external: false,
            target_external: false,
            operation: Operation::Copy,
            scope: Scope::Objects {
                keys: vec![key.into()],
            },
            target_prefix: String::new(),
            target_key: Some(self.target_key.clone()),
            permissions: serde_json::from_value(Value::String(self.permissions.clone()))?,
            conflict: Conflict::Overwrite,
            propagate_deletes: false,
        };
        options.validate()?;
        Ok(options)
    }

    async fn authorize(&self, db: &mut Tx<'_>, options: &Options) -> Result<()> {
        let grant = redundancy_transactions::authority(options, self.authority.0.clone());
        transactions::lock_media_scope(db, &grant).await?;
        manager(db, self.target_bucket, &self.target_key, self.link_id).await?;
        ensure!(
            q::link(self.link_id, options.source_bucket, self.target_bucket)
                .fetch_optional(&mut **db)
                .await?
                .is_some_and(|(revision, sync)| sync && revision >= self.link_revision),
            Error::OperationAborted
        );
        ensure!(
            transfer::profile(self.target_bucket)
                .fetch_optional(&mut **db)
                .await?
                == Some((self.storage_id, self.profile_revision)),
            Error::OperationAborted
        );
        Ok(())
    }
}

pub(super) async fn prepare(
    db: &mut Tx<'_>,
    publication: Uuid,
    bucket: Uuid,
    key: &str,
    stream: Id,
    intent: &Value,
) -> Result<(String, bool)> {
    let (phase, saved, early) = q::identity(publication).fetch_one(&mut **db).await?;
    let hash = blake3::hash(&serde_json::to_vec(intent)?);
    if phase != "admitted" {
        ensure!(
            saved.as_deref() == Some(hash.as_bytes().as_slice()) && phase != "aborted",
            Error::OperationAborted
        );
        return Ok((phase, early));
    }
    let original = q::original(publication, stream)
        .fetch_one(&mut **db)
        .await?;
    let snapshot = original.snapshot();
    let (primary, _) = transfer::profile(bucket).fetch_one(&mut **db).await?;
    let mode = transfer::mode().fetch_one(&mut **db).await?;
    let early = q::locally_pending(stream).fetch_one(&mut **db).await?.0;
    let saved = json!({"request":intent,"audit":crate::model::audit::current(),
        "receipt":crate::model::object::RECEIPT.try_with(Clone::clone).ok()});
    ensure!(
        q::begin(publication, &saved, hash.as_bytes(), early)
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if !early && intent["operation"] == "write" && !intent["completion"].is_null() {
        let (upload, hash, _, token): (Uuid, String, Value, Uuid) =
            serde_json::from_value(intent["completion"].clone())?;
        ensure!(
            q::hold_completion(stream, upload, &hash, token)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    while let Some(target) = q::target(publication, "admitted")
        .fetch_optional(&mut **db)
        .await?
    {
        let options = target.options(bucket, key)?;
        target.authorize(db, &options).await?;
        let task = transactions::enqueue_replica(db, target.target_bucket, json!({"operation":"replication","phase":"plan","external":false,"storage_id":target.storage_id,"execution_mode":mode})).await?;
        transfer::create(
            task,
            &target.authority,
            &options,
            Some(target.storage_id),
            Some(target.profile_revision),
            Some(primary),
        )
        .execute(&mut **db)
        .await?;
        transfer::insert_item(
            task,
            key,
            &target.target_key,
            Some(stream),
            target.expected_version,
            &snapshot,
            "planning",
            None,
        )
        .execute(&mut **db)
        .await?;
        let candidate = transactions::reserve_replica(
            db,
            &StreamCreate {
                authority: options.authority(target.authority.0),
                bucket: target.target_bucket,
                key: target.target_key.clone(),
                purpose: StreamPurpose::Internal,
                metadata: snapshot.metadata.clone(),
                public: false,
                completion: None,
                owner: Some((task, Uuid::nil())),
            },
            snapshot.size,
        )
        .await?;
        transactions::selected_object(
            db,
            target.target_bucket,
            &target.target_key,
            target.expected_version,
        )
        .await?;
        ensure!(
            q::reserve_item(task, candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        ensure!(
            q::delegate(publication, target.link_id, task, candidate)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        ensure!(
            q::start_child(task)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
    }
    Ok(("preparing".into(), early))
}

pub(super) async fn manager(db: &mut Tx<'_>, bucket: Uuid, key: &str, link: Uuid) -> Result<()> {
    ensure!(
        redundancy_queries::manager(bucket, key)
            .fetch_optional(&mut **db)
            .await?
            .is_some_and(|(owner, _)| owner == link),
        Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn park(db: &mut Tx<'_>, task: crate::model::task::Execution) -> Result<()> {
    ensure!(
        q::park(task).execute(&mut **db).await?.rows_affected() == 1,
        Error::OperationAborted
    );
    Ok(())
}

async fn finish(
    db: &mut Tx<'_>,
    publication: Uuid,
    bucket: Uuid,
    key: &str,
    stream: Id,
) -> Result<()> {
    while let Some(target) = q::target(publication, "prepared")
        .fetch_optional(&mut **db)
        .await?
    {
        let options = target.options(bucket, key)?;
        target.authorize(db, &options).await?;
        let candidate = target.candidate.ok_or(Error::SourceUnavailable)?;
        transactions::install_replica(
            db,
            target.target_bucket,
            &target.target_key,
            target.expected_version,
            candidate,
        )
        .await?;
        ensure!(
            q::installed(publication, target.link_id)
                .execute(&mut **db)
                .await?
                .rows_affected()
                == 1,
            Error::OperationAborted
        );
        wake_link(db, target.link_id).await?;
    }
    ensure!(
        q::finish(publication, &json!({"stream":stream}))
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    Ok(())
}

async fn wake_link(db: &mut Tx<'_>, link: Uuid) -> Result<()> {
    // Serialize with the link worker between selecting pending work and parking.
    transfer::task_lock(link).fetch_optional(&mut **db).await?;
    q::drained(link).execute(&mut **db).await?;
    Ok(())
}

pub(super) async fn prune(db: &mut Tx<'_>, ids: Vec<(Uuid,)>) -> Result<()> {
    for (id,) in ids {
        q::detach(id).execute(&mut **db).await?;
        q::prune(id).execute(&mut **db).await?;
    }
    Ok(())
}

pub(super) async fn recover_admissions(db: &mut Tx<'_>) -> Result<()> {
    // Abandoned bytes can outlive the admission that pinned their targets.
    while reap_abandoned(db, 64).await? == 64 {}
    Ok(())
}

pub(super) async fn reap_abandoned(db: &mut Tx<'_>, batch: i64) -> Result<usize> {
    let rows = q::abandoned_admissions(batch.clamp(1, 64))
        .fetch_all(&mut **db)
        .await?;
    let mut reaped = 0;
    for (stream,) in rows {
        if reap_admission(db, stream, None).await? {
            reaped += 1;
        }
    }
    Ok(reaped)
}

pub(super) async fn reap_admission(
    db: &mut Tx<'_>,
    stream: Id,
    rejection: Option<&str>,
) -> Result<bool> {
    if let Some((id, bucket)) = q::admission(stream, rejection.is_some())
        .fetch_optional(&mut **db)
        .await?
    {
        while let Some(target) = q::target(id, "admitted").fetch_optional(&mut **db).await? {
            q::abort_target(id, target.link_id)
                .execute(&mut **db)
                .await?;
            wake_link(db, target.link_id).await?;
        }
        q::forget_admission(
            id,
            &json!({"error":rejection.unwrap_or("operation_aborted")}),
        )
        .execute(&mut **db)
        .await?;
        if rejection.is_some() {
            transactions::discard_replica(db, bucket, stream).await?;
        }
        return Ok(true);
    }
    Ok(false)
}

pub(super) async fn failed(db: &mut Tx<'_>, task: Uuid, reason: &str) -> Result<()> {
    if let Some((publication, early)) = q::failed(task).fetch_optional(&mut **db).await? {
        if !early {
            let reason = match reason {
                "quota_exceeded" => "quota_exceeded",
                "access_revoked" | "access_denied" => "access_denied",
                "precondition_failed" => "precondition_failed",
                _ => "operation_aborted",
            };
            abort(db, publication, reason).await?;
        }
    }
    Ok(())
}

pub(super) async fn abort(db: &mut Tx<'_>, publication: Uuid, code: &str) -> Result<()> {
    let Some((bucket, _, source, _)) = q::root(publication).fetch_optional(&mut **db).await? else {
        return Ok(());
    };
    let saved = q::intent(publication).fetch_one(&mut **db).await?.0.0;
    if let Some(parent) = saved["request"]["task"].as_str() {
        q::fail_parent(Uuid::parse_str(parent)?, code)
            .execute(&mut **db)
            .await?;
    }
    let node = serde_json::from_value(saved.get("node").cloned().unwrap_or(Value::Null))?;
    transactions::abort_replica_node(db, node).await?;
    if let Some(stream) = source
        && saved["request"]["completion"].is_array()
    {
        q::release_completion(stream).execute(&mut **db).await?;
    }
    for phase in ["admitted", "preparing", "prepared"] {
        while let Some(target) = q::target(publication, phase)
            .fetch_optional(&mut **db)
            .await?
        {
            ensure!(
                q::abort_target(publication, target.link_id)
                    .execute(&mut **db)
                    .await?
                    .rows_affected()
                    == 1,
                Error::OperationAborted
            );
            wake_link(db, target.link_id).await?;
            if let Some(candidate) = target.candidate {
                transactions::discard_replica(db, target.target_bucket, candidate).await?;
            }
            if let Some(task) = target.task_id {
                q::cancel_child(task).execute(&mut **db).await?;
                transfer::task_state(task, "queued", None)
                    .execute(&mut **db)
                    .await?;
            }
        }
    }
    ensure!(
        q::abort(publication, &json!({"error":code}))
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if let Some(source) = source {
        transactions::discard_replica(db, bucket, source).await?;
    }
    Ok(())
}

pub(super) async fn publish(
    db: &mut Tx<'_>,
    publication: Uuid,
    bucket: Uuid,
    key: &str,
    stream: Id,
    intent: &Value,
) -> Result<()> {
    let (phase, early) = prepare(db, publication, bucket, key, stream, intent).await?;
    if phase == "published" {
        return Ok(());
    }
    let (ready, failed) = q::readiness(publication).fetch_one(&mut **db).await?;
    ensure!(!failed, Error::OperationAborted);
    if ready {
        finish(db, publication, bucket, key, stream).await
    } else if early {
        Ok(())
    } else {
        Err(crate::model::ReplicaPending(publication).into())
    }
}

pub(super) async fn prepared(
    db: &mut Tx<'_>,
    task: Uuid,
    candidate: Id,
    payload: &str,
) -> Result<()> {
    let publication = q::publication_for_task(task).fetch_one(&mut **db).await?.0;
    let (bucket, key, source, early) = q::root(publication).fetch_one(&mut **db).await?;
    ensure!(
        transfer::ready(candidate, payload)
            .fetch_one(&mut **db)
            .await?
            && !q::locally_pending(candidate).fetch_one(&mut **db).await?.0,
        Error::SourceUnavailable
    );
    ensure!(
        q::prepared(task, candidate)
            .execute(&mut **db)
            .await?
            .rows_affected()
            == 1,
        Error::OperationAborted
    );
    if q::readiness(publication).fetch_one(&mut **db).await?.0 {
        let source = source.ok_or(Error::SourceUnavailable)?;
        let saved = q::intent(publication).fetch_one(&mut **db).await?.0.0;
        let mut finalization = db.begin().await?;
        let outcome: Result<()> = async {
            let db = &mut finalization;
            if early {
                transactions::selected_object(db, bucket, &key, Some(source)).await?;
                finish(db, publication, bucket, &key, source).await?;
            } else if saved["request"]["operation"] == "task" {
                let task = serde_json::from_value(saved["request"]["task"].clone())?;
                ensure!(
                    q::wake(task).execute(&mut **db).await?.rows_affected() == 1,
                    Error::OperationAborted
                );
            } else if matches!(
                saved["request"]["operation"].as_str(),
                Some("write" | "copy" | "move")
            ) {
                let audit: Option<Id> = serde_json::from_value(saved["audit"].clone())?;
                let finalize = async {
                    let request = &saved["request"];
                    if matches!(request["operation"].as_str(), Some("copy" | "move")) {
                        let job: crate::model::object::StorageMigration =
                            serde_json::from_value(request["transfer"].clone())?;
                        let permit = serde_json::from_value(request["permit"].clone())?;
                        let apply = async {
                            if job.action == "move" {
                                transactions::move_object_in(
                                    db,
                                    &permit,
                                    job.transfer(),
                                    Some(publication),
                                )
                                .await
                            } else {
                                transactions::publish_copy_in(
                                    db,
                                    &permit,
                                    job.transfer(),
                                    source,
                                    job.public_read,
                                    serde_json::from_value(request["expected_public"].clone())?,
                                )
                                .await
                            }
                        };
                        return if let Some(receipt) =
                            serde_json::from_value(saved["receipt"].clone())?
                        {
                            crate::model::object::RECEIPT.scope(receipt, apply).await
                        } else {
                            apply.await
                        };
                    }
                    let header = |s: &Value| -> Result<_> {
                        Ok(s.as_str()
                            .map(|s| s3s::dto::ETagCondition::parse_http_header(s.as_bytes()))
                            .transpose()?)
                    };
                    let node =
                        serde_json::from_value(saved.get("node").cloned().unwrap_or(Value::Null))?;
                    transactions::validate_replica_node(db, source, node).await?;
                    ensure!(
                        transactions::publish_in(
                            db,
                            source,
                            header(&request["if_match"])?,
                            header(&request["if_none"])?,
                            serde_json::from_value(request["completion"].clone())?
                        )
                        .await?,
                        Error::OperationAborted
                    );
                    transactions::finish_replica_node(db, source, node).await
                };
                if let Some(audit) = audit {
                    crate::model::audit::scope(audit, finalize).await?
                } else {
                    finalize.await?
                }
            }
            Ok(())
        }
        .await;
        match outcome {
            Ok(()) => finalization.commit().await?,
            Err(error) => {
                finalization.rollback().await?;
                let Some(code) = crate::model::publish::replica_rejection(&error) else {
                    return Err(error);
                };
                if early && code != "operation_aborted" {
                    return Err(error);
                }
                abort(db, publication, code).await?;
            }
        }
    }
    Ok(())
}
