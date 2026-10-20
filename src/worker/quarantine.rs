use crate::model::Id;
use crate::{
    app::App,
    authorization::Principal,
    model::{
        source::{SourceOperation, SourceScope, SourceUpdate},
        task::Execution,
    },
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::sync::atomic::Ordering;

impl App {
    pub(crate) async fn source_list(
        &self,
        principal: &Principal,
        query: crate::model::source::SourceQuery,
    ) -> Result<crate::model::source::SourcePage> {
        self.authorization.admin(principal).await?.finish().await?;
        let limit = query.limit.unwrap_or(50);
        ensure!((1..=200).contains(&limit), s3s::s3_error!(InvalidArgument));
        let mut items = if query.source == crate::model::source::SourceType::Raw {
            let after = if query.after.is_empty() {
                Id::default()
            } else {
                Id::parse(&query.after, self.config.database.backend)?
            };
            self.raw
                .quarantines(query.storage_id, after, limit + 1)
                .await?
        } else {
            let after = if query.after.is_empty() {
                Id::default()
            } else {
                Id::parse(&query.after, self.config.database.backend)?
            };
            self.physical
                .quarantines(
                    query.storage_id,
                    query.source == crate::model::source::SourceType::Pack,
                    after,
                    limit + 1,
                )
                .await?
        };
        let next = if items.len() > limit as usize {
            Some(items[limit as usize - 1].scope.id.clone())
        } else {
            None
        };
        items.truncate(limit as usize);
        Ok(crate::model::source::SourcePage { items, next })
    }

    pub(crate) fn source_writable(&self) -> Result<()> {
        self.ensure_mode()?;
        ensure!(
            !self.draining.load(Ordering::Acquire),
            s3s::s3_error!(ServiceUnavailable)
        );
        Ok(())
    }

    pub(crate) async fn source_prepare(
        &self,
        principal: &Principal,
        scope: SourceScope,
        recheck: bool,
    ) -> Result<SourceOperation> {
        self.authorization.admin(principal).await?.finish().await?;
        self.source_writable()?;
        scope.validate()?;
        scope.identity()?.validate(self.config.database.backend)?;
        let snapshot = self.physical.source_snapshot(&scope).await?;
        ensure!(
            snapshot.state == "ready" && snapshot.quarantine_code.is_some() == recheck,
            s3s::s3_error!(OperationAborted)
        );
        let profile = self.storage_profiles.load(scope.storage_id).await?;
        let timeout = crate::config::seconds(&profile.integrity_batch_timeout)?;
        let _slot = self.slots.acquire().await?;
        let _profile = self
            .storage
            .profile_permit(scope.storage_id, "integrity")
            .await?;
        // Namespace/key failures are not proof that a physical object is corrupt.
        let local = self.storage.verify_source_namespace(&snapshot).await?;
        let code = if recheck {
            None
        } else {
            let bytes = match &snapshot.physical {
                crate::model::PhysicalSource::Chunk(c) => i64::from(c.raw_size),
                crate::model::PhysicalSource::Pack(p) => p.raw_size,
                crate::model::PhysicalSource::Raw(r) => r.size,
            };
            ensure!(
                self.acquire_profile_rate(scope.storage_id, &profile, 2, bytes)
                    .await?,
                s3s::s3_error!(SlowDown)
            );
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(timeout),
                self.storage.inspect_exact(&snapshot),
            )
            .await?;
            let error = result.err().ok_or_else(|| {
                s3s::s3_error!(InvalidArgument, "source passed full verification")
            })?;
            let code = error
                .downcast_ref::<crate::content::codec::IntegrityError>()
                .map(|e| e.code())
                .or_else(|| {
                    matches!(
                        error.downcast_ref::<object_store::Error>(),
                        Some(object_store::Error::NotFound { .. })
                    )
                    .then_some("remote_missing")
                });
            match code.filter(|code| crate::model::source::quarantine_code(code)) {
                Some(code) => Some(code.to_owned()),
                None => return Err(error),
            }
        };
        Ok(SourceOperation {
            principal: principal.clone(),
            mode: if self.role == "single" {
                "single"
            } else {
                "cluster"
            }
            .into(),
            snapshot,
            code,
            node: if local {
                self.node.get().map(|node| node.node_id)
            } else {
                None
            },
        })
    }

    pub(crate) async fn source_start(&self, operation: SourceOperation) -> Result<Value> {
        let _coord = self.coord.lock().await;
        self.source_writable()?;
        self.authorization
            .admin(&operation.principal)
            .await?
            .finish()
            .await?;
        let snapshot = self
            .physical
            .source_snapshot(&operation.snapshot.scope)
            .await?;
        ensure!(
            snapshot.fingerprint()? == operation.snapshot.fingerprint()?,
            s3s::s3_error!(OperationAborted)
        );
        let policy = if operation.code.is_some() {
            "quarantine"
        } else {
            "recheck"
        };
        let id = self.task_control.enqueue("integrity", None, json!({"policy":policy,"operation":policy,
            "storage_id":snapshot.scope.storage_id,"source":snapshot.summary()?,"source_location":snapshot.location(),
            "source_node":operation.node,"execution_mode":operation.mode,"source_operation":operation})).await?;
        self.wake_tasks.notify_waiters();
        Ok(json!({"task_id":id}))
    }

    pub(crate) async fn source_batch(&self, execution: Execution, detail: Value) -> Result<bool> {
        let operation: SourceOperation =
            serde_json::from_value(detail["source_operation"].clone())?;
        {
            let _coord = self.coord.lock().await;
            self.source_writable()?;
            self.physical
                .source_update(SourceUpdate {
                    execution,
                    operation: &operation,
                    apply: false,
                })
                .await?;
        }
        if operation.code.is_none() {
            let snapshot = &operation.snapshot;
            let profile = self
                .storage_profiles
                .load(snapshot.scope.storage_id)
                .await?;
            let bytes = match &snapshot.physical {
                crate::model::PhysicalSource::Chunk(c) => i64::from(c.raw_size),
                crate::model::PhysicalSource::Pack(p) => p.raw_size,
                crate::model::PhysicalSource::Raw(r) => r.size,
            };
            if !self
                .acquire_profile_rate(snapshot.scope.storage_id, &profile, 2, bytes)
                .await?
            {
                return Ok(false);
            }
            let _slot = self.slots.acquire().await?;
            let _profile = self
                .storage
                .profile_permit(operation.snapshot.scope.storage_id, "integrity")
                .await?;
            self.storage
                .verify_source_namespace(&operation.snapshot)
                .await?;
            self.storage.inspect_exact(&operation.snapshot).await?;
        }
        #[cfg(feature = "fault-injection")]
        crate::faults::point("source-before-commit").await;
        let _coord = self.coord.lock().await;
        self.source_writable()?;
        self.physical
            .source_update(SourceUpdate {
                execution,
                operation: &operation,
                apply: true,
            })
            .await?;
        Ok(false)
    }
}
