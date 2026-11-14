use crate::{
    app::App,
    authorization::Principal,
    model::journal::restore::NativeRecovery,
    storage::{model, native::recovery::scan::Scanner},
};
use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams)]
pub(crate) struct Query {
    pub media: Uuid,
    pub after: Option<Uuid>,
}

#[derive(Serialize, ToSchema)]
#[schema(as = StorageRecoverySource)]
pub(crate) struct Entry {
    id: Uuid,
    prefix: Option<String>,
    revision: Option<String>,
    available: bool,
}

#[derive(Serialize, ToSchema)]
#[schema(as = StorageRecoverySources)]
pub(crate) struct Page {
    entries: Vec<Entry>,
    next: Option<Uuid>,
}

pub(crate) async fn scanner(app: &App, cluster: Uuid) -> Result<Arc<Scanner>> {
    ensure!(
        app.deployment_mode() != "single",
        "native recovery requires cluster mode"
    );
    Scanner::open(
        app.storage_clusters.clone(),
        app.storage.native_control.clone(),
        cluster,
    )
    .await
}

pub(crate) async fn list(
    app: &App,
    principal: &Principal,
    cluster: Uuid,
    query: Query,
) -> Result<Page> {
    app.authorization.admin(principal).await?.finish().await?;
    let scan = scanner(app, cluster).await?;
    let mut stream = scan.namespaces(query.media).await?;
    let mut entries = BTreeMap::new();
    while let Some(entry) = stream.try_next().await? {
        let Ok(id) = Uuid::parse_str(&entry.name) else {
            continue;
        };
        if query.after.is_some_and(|after| id <= after) {
            continue;
        }
        let proof = entry.proof.as_ref();
        entries.insert(
            id,
            Entry {
                id,
                prefix: proof.map(|p| p.publication.prefix.clone()),
                revision: proof.map(|p| p.publication.revision.to_string()),
                available: entry.error.is_none() && proof.is_some(),
            },
        );
        if entries.len() > 101 {
            entries.pop_last();
        }
    }
    let next = if entries.len() > 100 {
        entries.pop_last();
        entries.last_key_value().map(|(id, _)| *id)
    } else {
        None
    };
    Ok(Page {
        entries: entries.into_values().collect(),
        next,
    })
}

pub(crate) async fn collect(
    app: &App,
    id: Uuid,
    cluster: Uuid,
    namespace: Uuid,
) -> Result<(Arc<Scanner>, NativeRecovery)> {
    let scan = scanner(app, cluster).await?;
    let (current, targets) = app.storage_clusters.placement_snapshot(cluster).await?;
    ensure!(
        current.manifest == scan.anchor,
        "recovery membership changed"
    );
    let now = chrono::Utc::now().timestamp_micros();
    let media: Vec<_> = targets
        .into_iter()
        .filter(|target| {
            target.run.is_some() && target.expires > now && target.credential.is_some()
        })
        .map(|target| target.device.media_id)
        .collect();
    let head = scan.collect(id, namespace, &media).await?;
    ensure!(
        head.publication.root.is_some(),
        "recovery root mapping is missing"
    );
    Ok((scan, NativeRecovery { catalog: id, head }))
}

pub(crate) fn connect(app: &App, source: &model::Connection) -> Result<model::Store> {
    if source.r#type == "mokyu" {
        source
            .cluster_id
            .context("native recovery cluster is missing")?;
        model::Store::connect_native(
            source,
            crate::storage::journal::source::options(),
            crate::storage::native::store::Authority::Control(app.storage.native_control.clone()),
            app.config.instance.region.clone(),
        )
    } else {
        model::Store::connect(source, crate::storage::journal::source::options())
    }
}
