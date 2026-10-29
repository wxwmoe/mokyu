use super::Identity;
use crate::{
    app::App,
    content::codec,
    http::HttpError,
    model::external::{Create, ExternalBucket, Page, Patch, Save},
};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct Filter {
    project: Option<Uuid>,
    after: Option<Uuid>,
    limit: Option<usize>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalBucketRevision)]
pub(super) struct Revision {
    revision: i64,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalTestRequest)]
pub(super) struct TestRequest {
    revision: i64,
    node_id: Option<Uuid>,
}

#[utoipa::path(post, path="/api/v1/external-buckets/{id}/test", operation_id="external_test_start", params(("id"=Uuid, Path)), request_body=TestRequest, responses((status=202, body=super::maintenance::TaskStarted)))]
pub(super) async fn start_test(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<TestRequest>,
) -> Result<(StatusCode, Json<super::maintenance::TaskStarted>), HttpError> {
    actor.recent(&app).await?;
    if input.revision <= 0 || app.role == "single" && input.node_id.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let record = app.external_buckets.record(&actor.principal, id).await?;
    if record.bucket.revision != input.revision {
        return Err(crate::model::Error::OperationAborted.into());
    }
    let snapshot = crate::model::external_test::Snapshot {
        external_id: id,
        revision: input.revision,
        connection: record.bucket.connection,
        access_key: codec::unprotect(
            &record.access_encrypted,
            &app.secrets.credential_key,
            format!("external-{id}-access").as_bytes(),
        )?,
        secret_key: codec::unprotect(
            &record.secret_encrypted,
            &app.secrets.credential_key,
            format!("external-{id}-secret").as_bytes(),
        )?,
    };
    let task_id = app
        .external_buckets
        .start_test(
            &actor.principal,
            crate::model::external_test::Start {
                external_id: id,
                revision: input.revision,
                node: input.node_id,
                connection_encrypted: snapshot.seal(&app.secrets.credential_key)?,
                report: crate::model::external_test::Report::new(snapshot.connection),
            },
        )
        .await?;
    app.wake_tasks.notify_one();
    Ok((
        StatusCode::ACCEPTED,
        Json(super::maintenance::TaskStarted { task_id }),
    ))
}

#[utoipa::path(get, path="/api/v1/external-buckets/{id}/test", operation_id="external_test_latest", params(("id"=Uuid, Path)), responses((status=200, body=Option<crate::model::external_test::Test>)))]
pub(super) async fn latest_test(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Option<crate::model::external_test::Test>>, HttpError> {
    Ok(Json(
        app.external_buckets
            .latest_test(&actor.principal, id)
            .await?,
    ))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = ExternalTestStop)]
pub(super) struct TestStop {
    test_id: Uuid,
}

#[utoipa::path(post, path="/api/v1/external-buckets/{id}/test/stop", operation_id="external_test_stop", params(("id"=Uuid, Path)), request_body=TestStop, responses((status=204)))]
pub(super) async fn stop_test(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<TestStop>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    app.external_buckets
        .stop_test(&actor.principal, id, input.test_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path="/api/v1/external-tests/{id}", operation_id="external_test_get", params(("id"=Uuid, Path)), responses((status=200, body=crate::model::external_test::Test)))]
pub(super) async fn get_test(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<crate::model::external_test::Test>, HttpError> {
    Ok(Json(app.external_buckets.test(&actor.principal, id).await?))
}

fn invalid(error: anyhow::Error) -> s3s::S3Error {
    s3s::s3_error!(InvalidArgument, "{error}")
}

#[utoipa::path(get, path="/api/v1/external-buckets", operation_id="external_list", params(Filter), responses((status=200, body=Page)))]
pub(super) async fn list(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Query(filter): Query<Filter>,
) -> Result<Json<Page>, HttpError> {
    let limit = filter.limit.unwrap_or(100);
    if !(1..=200).contains(&limit) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok(Json(
        app.external_buckets
            .page(&actor.principal, filter.project, filter.after, limit)
            .await?,
    ))
}

#[utoipa::path(get, path="/api/v1/external-buckets/{id}", operation_id="external_get", params(("id"=Uuid, Path)), responses((status=200, body=ExternalBucket)))]
pub(super) async fn get(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<ExternalBucket>, HttpError> {
    Ok(Json(app.external_buckets.view(&actor.principal, id).await?))
}

#[utoipa::path(post, path="/api/v1/external-buckets", operation_id="external_create", request_body=Create, responses((status=201, body=ExternalBucket)))]
pub(super) async fn create(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Json(input): Json<Create>,
) -> Result<(StatusCode, Json<ExternalBucket>), HttpError> {
    actor.recent(&app).await?;
    crate::storage::external::credentials(&input.access_key, &input.secret_key).map_err(invalid)?;
    let id = Uuid::now_v7();
    let save = Save {
        id,
        expected: None,
        project_id: input.project_id,
        name: input.name.trim().to_owned(),
        connection: input.connection,
        enabled: true,
        access_encrypted: codec::protect(
            input.access_key.as_bytes(),
            &app.secrets.credential_key,
            format!("external-{id}-access").as_bytes(),
        )?,
        secret_encrypted: codec::protect(
            input.secret_key.as_bytes(),
            &app.secrets.credential_key,
            format!("external-{id}-secret").as_bytes(),
        )?,
    };
    save.validate().map_err(invalid)?;
    Ok((
        StatusCode::CREATED,
        Json(app.external_buckets.save(&actor.principal, save).await?),
    ))
}

#[utoipa::path(patch, path="/api/v1/external-buckets/{id}", operation_id="external_update", params(("id"=Uuid, Path)), request_body=Patch, responses((status=200, body=ExternalBucket)))]
pub(super) async fn update(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Patch>,
) -> Result<Json<ExternalBucket>, HttpError> {
    actor.recent(&app).await?;
    let current = app.external_buckets.record(&actor.principal, id).await?;
    if current.bucket.revision != input.revision {
        return Err(crate::model::Error::OperationAborted.into());
    }
    let mut save = Save {
        id,
        expected: Some(input.revision),
        project_id: current.bucket.project_id,
        name: input
            .name
            .map(|v| v.trim().to_owned())
            .unwrap_or(current.bucket.name),
        connection: input.connection.unwrap_or(current.bucket.connection),
        enabled: input.enabled.unwrap_or(current.bucket.enabled),
        access_encrypted: current.access_encrypted,
        secret_encrypted: current.secret_encrypted,
    };
    if let Some(access) = input.access_key {
        crate::storage::external::credentials(&access, "check").map_err(invalid)?;
        save.access_encrypted = codec::protect(
            access.as_bytes(),
            &app.secrets.credential_key,
            format!("external-{id}-access").as_bytes(),
        )?;
    }
    if let Some(secret) = input.secret_key {
        crate::storage::external::credentials("check", &secret).map_err(invalid)?;
        save.secret_encrypted = codec::protect(
            secret.as_bytes(),
            &app.secrets.credential_key,
            format!("external-{id}-secret").as_bytes(),
        )?;
    }
    save.validate().map_err(invalid)?;
    Ok(Json(
        app.external_buckets.save(&actor.principal, save).await?,
    ))
}

#[utoipa::path(delete, path="/api/v1/external-buckets/{id}", operation_id="external_delete", params(("id"=Uuid, Path)), request_body=Revision, responses((status=204)))]
pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(id): Path<Uuid>,
    Json(input): Json<Revision>,
) -> Result<StatusCode, HttpError> {
    actor.recent(&app).await?;
    if input.revision <= 0 {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    app.external_buckets
        .delete(&actor.principal, id, input.revision)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
