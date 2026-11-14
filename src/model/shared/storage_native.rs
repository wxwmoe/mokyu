use super::{
    Backend,
    queries::{storage_native as q, storage_payload as physical},
    storage_payload,
};
use crate::model::{Error as ModelError, storage_native::*};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

#[cfg(test)]
#[path = "../../../tests/recovery/storage_native.rs"]
mod tests;

pub(super) async fn source(
    db: &mut super::queries::pack_work::Connection,
    namespace: Uuid,
    unit: &str,
    address: Uuid,
) -> Result<Option<(Uuid, String)>> {
    let Some(prefix) = q::prefix(namespace).fetch_optional(&mut *db).await? else {
        return Ok(None);
    };
    let layout = crate::storage::location::Layout::S3;
    let name = match unit {
        "chunk" => layout.chunk(address),
        "pack" => layout.pack(address),
        "raw" => layout.object(address),
        "fragment" => object_store::path::Path::from(crate::storage::fragment_key(address)),
        _ => anyhow::bail!("unknown native carrier"),
    };
    let prefix = object_store::path::Path::parse(prefix)?;
    let name = if prefix.as_ref().is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    };
    let (operation,) =
        super::queries::storage_journal::source(namespace, path(&name)?.to_vec(), &name)
            .fetch_optional(db)
            .await?
            .context("native source is not published")?;
    Ok(Some((operation, name)))
}

pub(super) async fn require_protection(tx: &mut physical::Tx<'_>, operation: Uuid) -> Result<()> {
    use crate::model::storage_cluster::Acknowledgement;
    let (policy, manifest) = q::protection_policy(operation).fetch_one(&mut **tx).await?;
    let value = storage_payload::current(tx, operation).await?;
    ensure!(value.state == "published", ModelError::SourceUnavailable);
    let mut degraded = false;
    if policy.degraded_acknowledgement.is_some() {
        for media in &value.intent.members {
            let member = manifest
                .members
                .iter()
                .find(|m| m.media_id == *media)
                .ok_or(ModelError::SourceUnavailable)?;
            if member.state != "available"
                || !super::queries::storage_cluster::node_active(tx, member.node_id).await?
            {
                degraded = true;
                break;
            }
        }
    }
    let acknowledgement = if degraded {
        policy
            .degraded_acknowledgement
            .unwrap_or(policy.acknowledgement)
    } else {
        policy.acknowledgement
    };
    let mut required = value.intent.clone();
    required.acknowledgement = acknowledgement;
    let count = q::confirmed_copies(
        operation,
        acknowledgement == Acknowledgement::All,
        required.layout.data_shards,
        acknowledgement != Acknowledgement::Data,
    )
    .fetch_one(&mut **tx)
    .await?;
    ensure!(
        count as u64 == required.required_files(),
        ModelError::StorageCopiesPending
    );
    Ok(())
}

pub(super) async fn publication(tx: &mut physical::Tx<'_>, id: crate::model::Id) -> Result<()> {
    if !q::native_stream(id).fetch_one(&mut **tx).await? {
        return Ok(());
    }
    let stream = super::queries::replica::candidate(id)
        .fetch_one(&mut **tx)
        .await?
        .stream;
    let mut start = 0;
    while start < stream.size {
        let (raw, extents) = if stream.payload_type == "raw" {
            (
                super::queries::node_read_raw(id, start, stream.size, 128)
                    .fetch_all(&mut **tx)
                    .await?
                    .into_iter()
                    .map(|row| (id, row))
                    .collect(),
                Vec::new(),
            )
        } else {
            (
                Vec::new(),
                super::queries::node_read_extents(id, start, stream.size, 128)
                    .fetch_all(&mut **tx)
                    .await?,
            )
        };
        let mapping = super::queries::sources::Sources::load(&mut **tx, raw, extents)
            .await?
            .mapping(Uuid::nil(), &stream, start, stream.size, true)?;
        ensure!(mapping.end > start, ModelError::SourceUnavailable);
        let mut checked = std::collections::BTreeSet::new();
        for segment in mapping.segments {
            let Some(namespace) = segment.source.storage()? else {
                continue;
            };
            let (unit, address) = segment.source.pin()?;
            if checked.insert((namespace, unit, address)) {
                if let Some((operation, _)) = source(&mut **tx, namespace, unit, address).await? {
                    require_protection(tx, operation).await?;
                }
            }
        }
        start = mapping.end;
    }
    Ok(())
}

fn lifetime(until: i64) -> Result<()> {
    let now = chrono::Utc::now().timestamp_micros();
    ensure!(
        until > now && until <= now + 90_000_000,
        "invalid native object lease"
    );
    Ok(())
}

pub(super) async fn initialized(
    tx: &mut physical::Tx<'_>,
    record: &crate::storage::model::Record,
) -> Result<()> {
    if record.r#type != "mokyu" {
        return Ok(());
    }
    let object = q::object(record.id, path("mokyu.json")?.to_vec())
        .fetch_optional(&mut **tx)
        .await?
        .context("native storage marker is missing")?;
    ensure!(object.1 == "mokyu.json", "native marker path mismatch");
    let payload = storage_payload::current(
        tx,
        object.2.context("native storage marker is not published")?,
    )
    .await?;
    ensure!(
        payload.state == "published"
            && Some(payload.intent.cluster) == record.cluster_id
            && payload.intent.namespace == record.id
            && payload.intent.area == crate::content::fragment::Area::Blocks,
        "native storage marker does not belong to this backend"
    );
    Ok(())
}

pub(super) async fn publish_carrier(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    path: &object_store::path::Path,
    owner: Uuid,
    bytes: u64,
) -> Result<()> {
    let prefix = q::prefix(namespace).fetch_one(&mut **tx).await?;
    publish_at(tx, namespace, &prefix, path, owner, bytes).await
}

pub(super) async fn published_raw_copy(
    tx: &mut physical::Tx<'_>,
    target: &crate::model::raw::RawSource,
) -> Result<bool> {
    if let Some((operation, _)) =
        source(&mut **tx, target.storage_id, "raw", target.address()).await?
    {
        let value = storage_payload::current(tx, operation).await?;
        ensure!(
            value.intent.total == u64::try_from(target.size)?,
            ModelError::OperationAborted
        );
        require_protection(tx, operation).await?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) async fn publish_staged(
    tx: &mut physical::Tx<'_>,
    storage: Option<Uuid>,
    fragment: Uuid,
    bytes: u64,
) -> Result<()> {
    if let Some(storage) = storage
        && let Some(prefix) = q::prefix(storage).fetch_optional(&mut **tx).await?
    {
        let path = object_store::path::Path::from(crate::storage::fragment_key(fragment));
        publish_at(tx, storage, &prefix, &path, fragment, bytes).await?;
    }
    Ok(())
}

pub(super) async fn part_carrier(
    tx: &mut physical::Tx<'_>,
    upload: Uuid,
    stream: crate::model::Id,
    storage: Uuid,
    expected: Option<Uuid>,
) -> Result<Option<Uuid>> {
    if q::prefix(storage)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let location = expected.unwrap_or_else(Uuid::now_v7);
    q::add_part_carrier(
        location,
        upload,
        stream,
        storage,
        chrono::Utc::now().timestamp_micros(),
    )
    .execute(&mut **tx)
    .await?;
    let saved = q::part_carrier(upload, stream, storage)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        expected.is_none_or(|id| id == saved),
        ModelError::NodeOperationConflict
    );
    Ok(Some(saved))
}

pub(super) async fn part_receipt(
    tx: &mut physical::Tx<'_>,
    upload: Uuid,
    stream: crate::model::Id,
    storage: Uuid,
    size: i64,
    value: &str,
) -> Result<()> {
    if q::prefix(storage)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        return Ok(());
    }
    let location = q::part_carrier(upload, stream, storage)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ModelError::NodeOperationConflict)?;
    ensure!(
        Uuid::parse_str(value)? == location && size >= 0,
        ModelError::NodeOperationConflict
    );
    publish_staged(tx, Some(storage), location, size as u64).await
}

pub(super) async fn collect_parts(
    tx: &mut physical::Tx<'_>,
    storage: Uuid,
    limit: usize,
) -> Result<()> {
    let Some(prefix) = q::prefix(storage).fetch_optional(&mut **tx).await? else {
        return Ok(());
    };
    let prefix = object_store::path::Path::parse(prefix)?;
    for location in q::unused_parts(
        storage,
        chrono::Utc::now().timestamp_micros() - 300_000_000,
        limit as i64,
    )
    .fetch_all(&mut **tx)
    .await?
    {
        let path = crate::storage::fragment_key(location);
        let path = if prefix.as_ref().is_empty() {
            path
        } else {
            format!("{prefix}/{path}")
        };
        remove_in(tx, storage, &path, None, None).await?;
        q::remove_part_carrier(location).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn multipart_candidate(
    tx: &mut physical::Tx<'_>,
    plan: &crate::model::node_native::Completion,
) -> Result<()> {
    let Some(prefix) = q::prefix(plan.upload.storage)
        .fetch_optional(&mut **tx)
        .await?
    else {
        return Ok(());
    };
    let path = crate::storage::location::Layout::S3.object(plan.upload.source);
    let prefix_path = object_store::path::Path::parse(&prefix)?;
    let name = if prefix_path.as_ref().is_empty() {
        path.to_string()
    } else {
        format!("{prefix_path}/{path}")
    };
    if let Some(object) = q::object(
        plan.upload.storage,
        crate::model::storage_native::path(&name)?.to_vec(),
    )
    .fetch_optional(&mut **tx)
    .await?
    {
        if let Some(operation) = object.3.or(object.2) {
            let payload = storage_payload::current(tx, operation).await?;
            if payload.state == "writing"
                && physical::complete(operation).fetch_one(&mut **tx).await? as u64
                    != payload.intent.required_files()
            {
                return Ok(());
            }
            publish_at(
                tx,
                plan.upload.storage,
                &prefix,
                &path,
                plan.upload.source,
                plan.size()?,
            )
            .await?;
        }
    }
    Ok(())
}

pub(super) async fn multipart_receipt(
    tx: &mut physical::Tx<'_>,
    work: &crate::model::node_native::Work,
    value: Option<&str>,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if q::prefix(work.upload.storage)
        .fetch_optional(&mut **tx)
        .await?
        .is_none()
    {
        return Ok(());
    }
    match &work.action {
        crate::model::node_native::Action::Initialize => {
            ensure!(
                Uuid::parse_str(value)? == work.upload.source,
                ModelError::NodeOperationConflict
            );
        }
        crate::model::node_native::Action::Part {
            stream, file, size, ..
        } => {
            ensure!(
                Uuid::parse_str(value)? == *file && *size >= 0,
                ModelError::NodeOperationConflict
            );
            part_receipt(
                tx,
                work.upload.upload,
                *stream,
                work.upload.storage,
                *size,
                value,
            )
            .await?;
        }
        crate::model::node_native::Action::Tail { size, .. } => {
            ensure!(
                Uuid::parse_str(value)? == work.upload.source && *size > 0,
                ModelError::NodeOperationConflict
            );
            publish_staged(
                tx,
                Some(work.upload.storage),
                work.upload.source,
                *size as u64,
            )
            .await?;
        }
        crate::model::node_native::Action::Abort => {
            return Err(ModelError::NodeOperationConflict.into());
        }
    }
    Ok(())
}

async fn publish_at(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    prefix: &str,
    path: &object_store::path::Path,
    owner: Uuid,
    bytes: u64,
) -> Result<()> {
    let prefix = object_store::path::Path::parse(prefix)?;
    let name = if prefix.as_ref().is_empty() {
        path.to_string()
    } else {
        format!("{prefix}/{path}")
    };
    publish_named(tx, namespace, &name, owner, bytes).await
}

async fn publish_named(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    name: &str,
    owner: Uuid,
    bytes: u64,
) -> Result<()> {
    let object = q::object(namespace, path(name)?.to_vec())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ModelError::StorageObjectMissing)?;
    ensure!(object.1 == name, "native carrier path digest collision");
    let operation = object.3.or(object.2).ok_or(ModelError::OperationAborted)?;
    let payload = storage_payload::current(tx, operation).await?;
    ensure!(
        payload.intent.total == bytes
            && payload.intent.area == crate::content::fragment::Area::Blocks,
        ModelError::OperationAborted
    );
    publish_in(tx, namespace, object.0, owner).await
}

pub(super) async fn publish_outputs(
    tx: &mut physical::Tx<'_>,
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    let mut prefixes = std::collections::BTreeMap::new();
    for output in outputs {
        let namespace = output
            .pack
            .as_ref()
            .and_then(|p| p.storage_id)
            .or_else(|| output.independent.as_ref().and_then(|c| c.storage_id));
        let Some(namespace) = namespace else {
            continue;
        };
        if !prefixes.contains_key(&namespace) {
            prefixes.insert(
                namespace,
                q::prefix(namespace).fetch_optional(&mut **tx).await?,
            );
        }
        let Some(prefix) = &prefixes[&namespace] else {
            continue;
        };
        let (path, owner, bytes) = if let Some(pack) = &output.pack {
            (
                crate::storage::location::Layout::S3.pack(pack.address()),
                pack.address(),
                u64::try_from(pack.stored_size.context("missing native pack size")?)?,
            )
        } else {
            let chunk = output
                .independent
                .as_ref()
                .context("missing native pack output")?;
            (
                crate::storage::location::Layout::S3.chunk(chunk.address()),
                chunk.address(),
                chunk.file_size()? as u64,
            )
        };
        publish_at(tx, namespace, prefix, &path, owner, bytes).await?;
    }
    Ok(())
}

pub(super) async fn publish_rewrite(
    tx: &mut physical::Tx<'_>,
    namespace: Option<Uuid>,
    path: object_store::path::Path,
    owner: Uuid,
    bytes: u64,
) -> Result<()> {
    if let Some(namespace) = namespace {
        if let Some(prefix) = q::prefix(namespace).fetch_optional(&mut **tx).await? {
            publish_at(tx, namespace, &prefix, &path, owner, bytes).await?;
        }
    }
    Ok(())
}

pub(super) async fn publish_raw(
    tx: &mut physical::Tx<'_>,
    source: crate::model::Id,
    bytes: u64,
) -> Result<()> {
    if let Some((namespace, location)) = q::raw_carrier(source).fetch_optional(&mut **tx).await? {
        publish_carrier(
            tx,
            namespace,
            &crate::storage::location::Layout::S3.object(location),
            location,
            bytes,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn publish_in(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    payload: Uuid,
    owner: Uuid,
) -> Result<()> {
    if let Some(job) = super::queries::journal_restore::native_pending(namespace)
        .fetch_optional(&mut **tx)
        .await?
    {
        ensure!(
            job == owner,
            s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
        );
    }
    let object = q::identified(namespace, payload)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ModelError::StorageObjectMissing)?;
    if let Some(operation) = object.2 {
        let binding = q::binding(operation).fetch_one(&mut **tx).await?;
        if binding.0 == owner {
            ensure!(
                storage_payload::current(tx, operation).await?.state == "published",
                ModelError::OperationAborted
            );
            return Ok(());
        }
    }
    let operation = object.3.ok_or(ModelError::OperationAborted)?;
    let binding = q::binding(operation).fetch_one(&mut **tx).await?;
    ensure!(
        binding.0 == owner && binding.2 == object.2,
        ModelError::OperationAborted
    );
    binding.1.check(object.2)?;
    storage_payload::publish_in(tx, operation).await?;
    ensure!(
        q::publish(
            namespace,
            payload,
            operation,
            chrono::Utc::now().timestamp_micros()
        )
        .execute(&mut **tx)
        .await?
        .rows_affected()
            == 1,
        ModelError::OperationAborted
    );
    if let Some(previous) = object.2 {
        if super::queries::storage_journal::held(previous)
            .fetch_one(&mut **tx)
            .await?
        {
            super::queries::storage_journal::track(operation, namespace, &object.1)
                .execute(&mut **tx)
                .await?;
        }
        physical::retire(previous).execute(&mut **tx).await?;
        super::queries::storage_journal::dirty(previous)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn remove_in(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    name: &str,
    expected: Option<Uuid>,
    before: Option<i64>,
) -> Result<()> {
    ensure!(
        super::queries::journal_restore::native_pending(namespace)
            .fetch_optional(&mut **tx)
            .await?
            .is_none(),
        s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
    );
    let object = q::object(namespace, path(name)?.to_vec())
        .fetch_optional(&mut **tx)
        .await?;
    let Some(object) = object else {
        ensure!(expected.is_none(), ModelError::StorageObjectChanged);
        return Ok(());
    };
    ensure!(object.1 == name, "native storage path digest collision");
    if let Some(expected) = expected {
        Condition::Match(expected).check(object.2)?;
    }
    ensure!(
        before.is_none_or(|before| object.5 <= before),
        ModelError::StorageObjectChanged
    );
    for operation in object.2.into_iter().chain(object.3) {
        physical::retire(operation).execute(&mut **tx).await?;
        super::queries::storage_journal::dirty(operation)
            .execute(&mut **tx)
            .await?;
    }
    q::clear(namespace, object.0).execute(&mut **tx).await?;
    Ok(())
}

async fn page_in(
    tx: &mut physical::Tx<'_>,
    namespace: Uuid,
    after: Option<&str>,
) -> Result<Vec<Metadata>> {
    let page = q::page(namespace, after.unwrap_or_default().to_owned())
        .fetch_all(&mut **tx)
        .await?;
    page.into_iter()
        .map(|row| {
            Ok(Metadata::from(Object {
                path: row.0,
                payload: storage_payload::payload((row.1, row.2, row.3, row.4))?,
                modified: row.5,
            }))
        })
        .collect()
}

#[async_trait::async_trait]
impl NativeObjects for Backend {
    async fn next_fill(&self, operation: Uuid) -> Result<Option<Fill>> {
        let mut tx = physical::begin(self).await?;
        let plan = storage_payload::fill_plan(&mut tx, operation).await?;
        tx.commit().await?;
        Ok(plan)
    }
    async fn authorize_fill(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        work: Uuid,
    ) -> Result<(Fill, i64)> {
        let mut tx = physical::begin(self).await?;
        super::queries::read_grants::access(self, &mut tx, node, key, run).await?;
        let (plan, expires) = q::storage_plan(&mut tx, node, run, work).await?;
        let crate::model::node_work::Plan::Replicate(plan) = plan else {
            anyhow::bail!(s3s::s3_error!(AccessDenied));
        };
        let payload = storage_payload::current(&mut tx, plan.operation).await?;
        ensure!(
            payload.state == "published" && payload.intent.namespace == plan.storage,
            ModelError::OperationAborted
        );
        tx.commit().await?;
        Ok((plan, expires))
    }
    async fn maintain(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        request: &access::Maintenance,
    ) -> Result<Vec<Metadata>> {
        use access::MaintenanceAction;
        request.validate()?;
        let mut tx = physical::begin(self).await?;
        super::queries::read_grants::access(self, &mut tx, node, key, run).await?;
        let (plan, _) = q::storage_plan(&mut tx, node, run, request.operation).await?;
        let prefix = q::prefix(request.storage)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let values = match &request.action {
            MaintenanceAction::StageCopy { path, size } => {
                let input = access::Upload {
                    operation: request.operation,
                    work: true,
                    batch: None,
                    storage: request.storage,
                    path: path.clone(),
                    size: *size,
                };
                ensure!(
                    matches!(
                        plan,
                        crate::model::node_work::Plan::Tier { .. }
                            | crate::model::node_work::Plan::RawTransfer { .. }
                    ),
                    s3s::s3_error!(AccessDenied)
                );
                let owner = input.for_work(&prefix, &plan)?;
                publish_named(&mut tx, request.storage, path, owner, *size).await?;
                Vec::new()
            }
            MaintenanceAction::Collect { payload } => {
                let crate::model::node_work::Plan::Collect { storage, items, .. } = &plan else {
                    anyhow::bail!(s3s::s3_error!(AccessDenied));
                };
                ensure!(
                    *storage == request.storage
                        && items.iter().any(|item| matches!(
                            item.source,
                            crate::model::node_work::Physical::Native { operation } if operation == *payload
                        )),
                    s3s::s3_error!(AccessDenied)
                );
                let value = storage_payload::current(&mut tx, *payload).await?;
                ensure!(
                    value.intent.namespace == request.storage
                        && value.intent.area == crate::content::fragment::Area::Blocks,
                    s3s::s3_error!(AccessDenied)
                );
                Vec::new()
            }
            MaintenanceAction::Delete { path } => {
                let before =
                    crate::model::storage_native::deletion(&plan, request.storage, &prefix, path)?;
                remove_in(&mut tx, request.storage, path, None, before).await?;
                Vec::new()
            }
            MaintenanceAction::List { after } => {
                let crate::model::node_work::Plan::SweepScan {
                    storage,
                    after: start,
                    limit,
                    ..
                } = &plan
                else {
                    anyhow::bail!(s3s::s3_error!(AccessDenied));
                };
                ensure!(
                    *storage == request.storage
                        && (1..=crate::model::node_work::SCAN_ITEMS).contains(limit),
                    s3s::s3_error!(AccessDenied)
                );
                let start = start.as_ref().map(|value| {
                    if prefix.is_empty() {
                        value.clone()
                    } else {
                        format!("{prefix}/{value}")
                    }
                });
                let after = after.as_deref().max(start.as_deref());
                let mut values = page_in(&mut tx, request.storage, after).await?;
                values.truncate(*limit);
                values
            }
        };
        tx.commit().await?;
        Ok(values)
    }

    async fn authorize_upload(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        request: &access::Upload,
    ) -> Result<access::UploadAccess> {
        request.validate()?;
        let mut tx = physical::begin(self).await?;
        super::queries::read_grants::access(self, &mut tx, node, key, run).await?;
        ensure!(
            super::queries::journal_restore::native_pending(request.storage)
                .fetch_optional(&mut *tx)
                .await?
                .is_none(),
            s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
        );
        let prefix = q::prefix(request.storage)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let cluster = q::cluster(request.storage).fetch_one(&mut *tx).await?;
        let (owner, expires) = if request.work {
            if let Some((plan, expires)) =
                q::multipart_work(&mut tx, node, run, request.operation).await?
            {
                (request.multipart(&prefix, &plan)?, expires)
            } else {
                let (plan, expires) =
                    q::storage_plan(&mut tx, node, run, request.operation).await?;
                (request.for_work(&prefix, &plan)?, expires)
            }
        } else {
            q::authorize_upload(&mut tx, node, run, request, &prefix).await?
        };
        tx.commit().await?;
        Ok(access::UploadAccess {
            owner,
            cluster,
            expires,
        })
    }

    async fn authorize_read(
        &self,
        node: &crate::cluster::NodeState,
        key: &crate::content::codec::Key,
        run: crate::model::session::Run,
        request: &access::Read,
    ) -> Result<i64> {
        use super::queries::read_grants;
        request.validate()?;
        let mut tx = physical::begin(self).await?;
        read_grants::access(self, &mut tx, node, key, run).await?;
        let prefix = q::prefix(request.storage)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let mut ranges = Vec::new();
        let mut expires = i64::MAX;
        if let Some(operation) = request.upload {
            if request.marker(&prefix)? {
                expires =
                    q::authorize_marker(&mut tx, node, run, operation, request.storage).await?;
                ranges.push(0..crate::storage::marker::MAX_BYTES as u64);
            } else {
                let (plan, deadline) =
                    q::multipart_completion(&mut tx, node, run, operation).await?;
                expires = deadline;
                request.include_completion(&prefix, &plan, &mut ranges)?;
            }
        }
        if let Some(operation) = request.work {
            if let Some((plan, deadline)) = q::multipart_work(&mut tx, node, run, operation).await?
            {
                ensure!(
                    request.storage == plan.upload.storage && request.marker(&prefix)?,
                    s3s::s3_error!(AccessDenied)
                );
                expires = deadline;
                ranges.push(0..crate::storage::marker::MAX_BYTES as u64);
            } else {
                let (plan, deadline) = q::storage_plan(&mut tx, node, run, operation).await?;
                expires = deadline;
                request.include_work(&prefix, &plan, &mut ranges)?;
            }
        }
        for window in &request.windows {
            let (mapping, deadline) = q::window(node, run, *window)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(ModelError::NodeExpired)?;
            expires = expires.min(deadline);
            request.include(&prefix, &mapping, &mut ranges)?;
        }
        request.check(&mut ranges)?;
        tx.commit().await?;
        Ok(expires)
    }

    async fn accept_receipts(
        &self,
        key: &crate::content::codec::Key,
        operation: Uuid,
        receipts: &[crate::model::storage_transfer::Completion],
    ) -> Result<()> {
        ensure!(
            !receipts.is_empty() && receipts.len() <= crate::model::storage_payload::BATCH_LIMIT,
            "invalid receipt proof batch"
        );
        let cluster = receipts[0].authorization.grant.cluster;
        let mut tx = physical::begin(self).await?;
        let allocation = super::storage_capacity::lock(self, &mut tx, cluster).await?;
        let manifest = &allocation.cluster.manifest;
        for receipt in receipts {
            let target = allocation
                .targets
                .iter()
                .find(|target| target.device.media_id == receipt.authorization.grant.media)
                .context("storage receipt target is not registered")?;
            receipt.verify(key, manifest, target)?;
            ensure!(
                receipt.receipt.receipt.header.operation == operation,
                ModelError::OperationAborted
            );
        }
        let values: Vec<_> = receipts
            .iter()
            .map(|receipt| receipt.receipt.receipt.clone())
            .collect();
        storage_payload::receipts_in(&mut tx, operation, &values).await?;
        let payload = storage_payload::current(&mut tx, operation).await?;
        allocation
            .received(&mut tx, &payload.intent, receipts)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn reserve_object(&self, input: &Write, until: i64) -> Result<Object> {
        input.validate()?;
        lifetime(until)?;
        let mut tx = physical::begin(self).await?;
        let allocation = super::storage_capacity::lock(self, &mut tx, input.intent.cluster).await?;
        let namespace = input.intent.namespace;
        if let Some(job) = super::queries::journal_restore::native_pending(namespace)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                job == input.owner
                    && (input.path == "mokyu.json"
                        || input.intent.area == crate::content::fragment::Area::Recovery),
                s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
            );
        }
        let cluster = q::cluster(namespace).fetch_optional(&mut *tx).await?;
        if cluster != Some(input.intent.cluster) {
            // Only control can create the root marker before committing its backend record.
            ensure!(
                cluster.is_none()
                    && input.path == "mokyu.json"
                    && input.condition == Condition::Create
                    && input.intent.area == crate::content::fragment::Area::Blocks
                    && input.intent.total <= crate::storage::marker::MAX_BYTES as u64
                    && q::unregistered(namespace).fetch_one(&mut *tx).await?,
                "native payload does not belong to this backend"
            );
        }
        let hash = path(&input.path)?.to_vec();
        q::insert(namespace, input.intent.payload, hash.clone(), &input.path)
            .execute(&mut *tx)
            .await?;
        let object = q::object(namespace, hash).fetch_one(&mut *tx).await?;
        ensure!(
            object.1 == input.path,
            "native storage path digest collision"
        );
        for operation in object.3.into_iter().chain(object.2) {
            let binding = q::binding(operation).fetch_one(&mut *tx).await?;
            if binding.0 == input.owner {
                let mut payload = storage_payload::current(&mut tx, operation).await?;
                ensure!(
                    binding.1.0 == input.condition
                        && payload.intent.total == input.intent.total
                        && payload.intent.area == input.intent.area
                        && payload.state != "retired",
                    ModelError::OperationAborted
                );
                ensure!(
                    input
                        .recovery
                        .is_none_or(|identity| payload.intent.payload == identity.payload
                            && payload.intent.revision == identity.revision),
                    "recovery write identity changed"
                );
                physical::renew(operation, until).execute(&mut *tx).await?;
                payload.write_until = payload.write_until.max(until);
                tx.commit().await?;
                return Ok(Object {
                    path: object.1,
                    payload,
                    modified: object.5,
                });
            }
        }
        input.condition.check(object.2)?;
        if let Some(operation) = object.3 {
            let old = storage_payload::current(&mut tx, operation).await?;
            ensure!(
                old.write_until <= chrono::Utc::now().timestamp_micros(),
                ModelError::OperationAborted
            );
            physical::retire(operation).execute(&mut *tx).await?;
            super::queries::storage_journal::dirty(operation)
                .execute(&mut *tx)
                .await?;
        }
        let mut intent = input.intent.clone();
        intent.payload = object.0;
        intent.revision = object
            .4
            .checked_add(1)
            .context("native object revision exhausted")? as u64;
        ensure!(
            input
                .recovery
                .is_none_or(|identity| identity.payload == intent.payload
                    && identity.revision == intent.revision),
            "recovery write revision changed"
        );
        allocation.select(&mut intent, input.region.as_deref())?;
        if input.path == "mokyu.json" {
            intent.acknowledgement = crate::model::storage_cluster::Acknowledgement::All;
        }
        let payload = storage_payload::prepare_in(&mut tx, &intent, until).await?;
        allocation.reserve(&mut tx, &intent, true).await?;
        q::bind(intent.operation, input.owner, &input.condition, object.2)
            .execute(&mut *tx)
            .await?;
        q::reserve(
            namespace,
            object.0,
            intent.revision as i64,
            intent.operation,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Object {
            path: object.1,
            payload,
            modified: object.5,
        })
    }

    async fn reserve_targets(&self, operation: Uuid) -> Result<Vec<Uuid>> {
        let payload = crate::model::storage_payload::StoragePayloads::get(self, operation).await?;
        let mut tx = physical::begin(self).await?;
        let allocation =
            super::storage_capacity::lock(self, &mut tx, payload.intent.cluster).await?;
        let payload = storage_payload::current(&mut tx, operation).await?;
        ensure!(payload.state != "retired", ModelError::OperationAborted);
        let targets = allocation.reserve(&mut tx, &payload.intent, false).await?;
        tx.commit().await?;
        Ok(targets)
    }

    async fn find_write(
        &self,
        namespace: Uuid,
        name: &str,
        owner: Uuid,
    ) -> Result<Option<(Object, Condition)>> {
        let mut tx = physical::begin(self).await?;
        if let Some(job) = super::queries::journal_restore::native_pending(namespace)
            .fetch_optional(&mut *tx)
            .await?
        {
            ensure!(
                job == owner,
                s3s::s3_error!(ServiceUnavailable, "storage takeover is not complete")
            );
        }
        let Some(object) = q::object(namespace, path(name)?.to_vec())
            .fetch_optional(&mut *tx)
            .await?
        else {
            return Ok(None);
        };
        ensure!(object.1 == name, "native storage path digest collision");
        for operation in object.3.into_iter().chain(object.2) {
            let binding = q::binding(operation).fetch_one(&mut *tx).await?;
            if binding.0 == owner {
                let payload = storage_payload::current(&mut tx, operation).await?;
                ensure!(payload.state != "retired", ModelError::OperationAborted);
                tx.commit().await?;
                return Ok(Some((
                    Object {
                        path: object.1,
                        payload,
                        modified: object.5,
                    },
                    binding.1.0,
                )));
            }
        }
        Ok(None)
    }

    async fn read_object(
        &self,
        namespace: Uuid,
        name: &str,
        owner: Option<Uuid>,
        until: i64,
    ) -> Result<Object> {
        lifetime(until)?;
        let mut tx = physical::begin(self).await?;
        let object = q::object(namespace, path(name)?.to_vec())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ModelError::StorageObjectMissing)?;
        ensure!(object.1 == name, "native storage path digest collision");
        let mut selected = object.2;
        if let (Some(owner), Some(pending)) = (owner, object.3) {
            if q::binding(pending).fetch_one(&mut *tx).await?.0 == owner {
                selected = Some(pending);
            }
        }
        let operation = selected.ok_or(ModelError::StorageObjectMissing)?;
        let mut payload = storage_payload::current(&mut tx, operation).await?;
        ensure!(
            payload.state != "retired" && (payload.state == "published" || selected == object.3),
            ModelError::OperationAborted
        );
        physical::protect(operation, until)
            .execute(&mut *tx)
            .await?;
        payload.protected_until = payload.protected_until.max(until);
        tx.commit().await?;
        Ok(Object {
            path: object.1,
            payload,
            modified: object.5,
        })
    }

    async fn resume_object(
        &self,
        namespace: Uuid,
        name: &str,
        operation: Uuid,
        until: i64,
    ) -> Result<Object> {
        lifetime(until)?;
        let mut tx = physical::begin(self).await?;
        let object = q::object(namespace, path(name)?.to_vec())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ModelError::StorageObjectMissing)?;
        let mut payload = storage_payload::current(&mut tx, operation).await?;
        ensure!(
            object.1 == name
                && payload.intent.namespace == namespace
                && payload.intent.payload == object.0
                && matches!(payload.state.as_str(), "published" | "retired")
                && payload.protected_until > chrono::Utc::now().timestamp_micros(),
            ModelError::OperationAborted
        );
        physical::protect(operation, until)
            .execute(&mut *tx)
            .await?;
        payload.protected_until = payload.protected_until.max(until);
        tx.commit().await?;
        Ok(Object {
            path: object.1,
            payload,
            modified: object.5,
        })
    }

    async fn publish_object(&self, namespace: Uuid, payload: Uuid, owner: Uuid) -> Result<()> {
        let mut tx = physical::begin(self).await?;
        publish_in(&mut tx, namespace, payload, owner).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_object(
        &self,
        namespace: Uuid,
        name: &str,
        expected: Option<Uuid>,
    ) -> Result<()> {
        let mut tx = physical::begin(self).await?;
        remove_in(&mut tx, namespace, name, expected, None).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_objects(&self, namespace: Uuid, after: Option<&str>) -> Result<Vec<Metadata>> {
        let mut tx = physical::begin(self).await?;
        let values = page_in(&mut tx, namespace, after).await?;
        tx.commit().await?;
        Ok(values)
    }
}
