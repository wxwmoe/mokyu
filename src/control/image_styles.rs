use super::Identity;
use crate::{
    app::App,
    authorization::Principal,
    http::HttpError,
    model::{Error, image_styles::*},
    processing::image::Pipeline,
};
use anyhow::{Result, ensure};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct Filter {
    after: Option<String>,
    prefix: Option<String>,
    limit: Option<u16>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImagePreview)]
pub(super) struct Preview {
    key: String,
    revision: i64,
    rule: Pipeline,
}
#[utoipa::path(post,path="/api/v1/buckets/{bucket}/image-processing/preview",operation_id="image_style_preview",params(("bucket"=Uuid,Path)),request_body=Preview,responses((status=200,content_type="image/*"),(status=403),(status=409),(status=415),(status=503)))]
pub(super) async fn preview(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    headers: axum::http::HeaderMap,
    Json(input): Json<Preview>,
) -> Result<axum::response::Response, HttpError> {
    use crate::{authorization::Action, processing::image::job};
    use axum::http::StatusCode;
    if input.key.is_empty() || input.key.len() > 1024 || input.key.contains('\0') {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let settings = app
        .image_styles
        .selected(&actor.principal, bucket, &[])
        .await?
        .settings;
    if settings.revision != input.revision {
        return Err(Error::OperationAborted.into());
    }
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Read)
        .await?;
    let (stream, pin) = app.current(bucket, &input.key).await?;
    if !stream.public_read {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    let version = stream.id;
    let product = job::execute(
        app.clone(),
        stream,
        pin,
        input.rule,
        headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        settings.policy.input(),
        4 * 1024 * 1024,
    )
    .await
    .map_err(|error| {
        if error.downcast_ref::<s3s::S3Error>().is_some() {
            HttpError(error)
        } else {
            tracing::debug!(%error,"image preview unavailable");
            crate::http::problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "PreviewUnavailable")
        }
    })?;
    actor
        .principal
        .require(app.authorization.as_ref(), bucket, Action::Read)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("image-before-validation").await;
    let latest = app
        .image_styles
        .selected(&actor.principal, bucket, &[])
        .await?
        .settings;
    if latest.revision != input.revision {
        return Err(Error::OperationAborted.into());
    }
    let (current, _pin) = app.current(bucket, &input.key).await?;
    if current.id != version || !current.public_read {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok(axum::response::Response::builder()
        .header(
            "content-type",
            format!("image/{}", product.output.format.word()),
        )
        .header("content-length", product.output.bytes)
        .header("cache-control", "private, no-store")
        .header("x-content-type-options", "nosniff")
        .header(
            "x-mokyu-image-source",
            serde_json::to_string(&product.source)?,
        )
        .header(
            "x-mokyu-image-output",
            serde_json::to_string(&product.image)?,
        )
        .header("x-mokyu-image-version", version.to_string())
        .header(
            "x-mokyu-image-source-bytes",
            product.source_bytes.to_string(),
        )
        .header("x-mokyu-image-elapsed-ms", product.elapsed_ms.to_string())
        .body(product.body().await?)?)
}

#[utoipa::path(get,path="/api/v1/buckets/{bucket}/image-processing/styles",operation_id="image_styles",params(("bucket"=Uuid,Path),Filter),responses((status=200,body=Page)))]
pub(super) async fn page(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(q): Query<Filter>,
) -> Result<Json<Page>, HttpError> {
    Ok(Json(
        app.image_styles
            .page(
                &actor.principal,
                bucket,
                q.after.as_deref().unwrap_or(""),
                q.prefix.as_deref().unwrap_or(""),
                q.limit.unwrap_or(50),
            )
            .await?,
    ))
}
#[utoipa::path(put,path="/api/v1/buckets/{bucket}/image-processing",operation_id="image_settings_save",params(("bucket"=Uuid,Path)),request_body=Update,responses((status=200,body=Settings)))]
pub(super) async fn save(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Update>,
) -> Result<Json<Settings>, HttpError> {
    app.writable()?;
    Ok(Json(
        app.image_styles
            .update(&actor.principal, bucket, &input, None, false)
            .await
            .map_err(validation_error)?,
    ))
}
#[utoipa::path(post,path="/api/v1/buckets/{bucket}/image-processing/styles/import",operation_id="image_styles_import",params(("bucket"=Uuid,Path)),request_body=Import,responses((status=200,body=Imported)))]
pub(super) async fn import(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Import>,
) -> Result<Json<Imported>, HttpError> {
    Ok(Json(
        import_styles(&app, &actor.principal, bucket, input).await?,
    ))
}
pub(crate) async fn import_styles(
    app: &App,
    principal: &Principal,
    bucket: Uuid,
    input: Import,
) -> Result<Imported> {
    ensure!(
        input.prefix.len() <= 128 && input.text.len() <= PAGE * (Pipeline::MAX_BODY + 160),
        s3s::s3_error!(InvalidArgument, "image import batch is too large")
    );
    let lines: Vec<_> = input
        .text
        .trim_start_matches('\u{feff}')
        .lines()
        .take(PAGE + 1)
        .collect();
    ensure!(
        !lines.is_empty() && lines.len() <= PAGE,
        s3s::s3_error!(InvalidArgument, "import between 1 and 128 lines per batch")
    );
    let picked = input
        .selected
        .as_ref()
        .map(|n| n.iter().copied().collect::<BTreeSet<u16>>());
    if let Some(s) = &picked {
        ensure!(
            s.len() == input.selected.as_ref().unwrap().len()
                && !s.is_empty()
                && s.iter().all(|i| *i > 0 && usize::from(*i) <= lines.len()),
            s3s::s3_error!(InvalidArgument, "invalid selected image lines")
        );
    }
    let digest = blake3::hash(&serde_json::to_vec(&(
        bucket,
        input.revision,
        &input.prefix,
        &input.text,
        input.conflict,
        &picked,
    ))?)
    .to_hex()
    .to_string();
    let mut rows = vec![];
    let mut parsed = BTreeMap::new();
    let mut seen = BTreeMap::new();
    for (i, raw) in lines.into_iter().enumerate() {
        let line = (i + 1) as u16;
        if raw.trim().is_empty() {
            continue;
        }
        let body = raw
            .split_once(',')
            .and_then(|(_, r)| r.split_once(':'))
            .map(|(_, r)| r.trim())
            .unwrap_or("");
        let mut row = Line {
            line,
            syntax: body.split('/').next().unwrap_or("").to_owned(),
            name: String::new(),
            body: None,
            outcome: "error".into(),
            error: None,
        };
        match parse_line(raw, &input.prefix) {
            Ok(style) => {
                row.name = style.name.clone();
                row.body = Some(style.rule.text());
                *seen.entry(style.name.clone()).or_insert(0usize) += 1;
                parsed.insert(line, style);
            }
            Err(e) => row.error = Some(e.to_string()),
        }
        rows.push(row);
    }
    ensure!(
        !rows.is_empty(),
        s3s::s3_error!(InvalidArgument, "no image rules in batch")
    );
    if let Some(s) = &picked {
        ensure!(
            s.iter().all(|i| rows.iter().any(|r| r.line == *i)),
            s3s::s3_error!(InvalidArgument, "selected line is empty")
        );
    }
    let names = seen.keys().cloned().collect::<Vec<_>>();
    let snapshot = app.image_styles.selected(principal, bucket, &names).await?;
    let existing = snapshot
        .styles
        .into_iter()
        .map(|s| (s.name.clone(), s))
        .collect::<BTreeMap<_, _>>();
    let revision = snapshot.settings.revision;
    if input.apply {
        app.writable()?;
        if input.revision != Some(revision) {
            if let Some(r) = app
                .image_styles
                .imported(principal, bucket, &digest)
                .await?
            {
                for line in &mut rows {
                    line.outcome = if picked.as_ref().is_none_or(|s| s.contains(&line.line)) {
                        "confirmed"
                    } else {
                        "ignored"
                    }
                    .into();
                }
                return Ok(Imported {
                    revision: r,
                    applied: true,
                    rows,
                });
            }
            return Err(Error::OperationAborted.into());
        }
    }
    let mut edits = vec![];
    let mut errors = false;
    for row in &mut rows {
        let chosen = picked.as_ref().is_none_or(|s| s.contains(&row.line));
        if seen.get(&row.name).is_some_and(|n| *n > 1) {
            row.error = Some("duplicate name in import batch".into());
        }
        if !chosen {
            row.outcome = "ignored".into();
            continue;
        }
        if row.error.is_some() {
            errors = true;
            continue;
        }
        let mut style = parsed
            .remove(&row.line)
            .expect("successfully parsed image line");
        if let Some(old) = existing.get(&row.name) {
            match input.conflict {
                Conflict::Reject => {
                    row.error = Some("style already exists".into());
                    errors = true;
                    continue;
                }
                Conflict::Skip => {
                    row.outcome = "skipped".into();
                    continue;
                }
                Conflict::Replace => {
                    style.enabled = old.enabled;
                }
            }
            if &style == old {
                row.outcome = "unchanged".into();
                continue;
            }
            row.outcome = "replace".into();
        } else {
            row.outcome = "create".into();
        }
        edits.push(Edit::Put { style });
    }
    let apply = input.apply && !errors;
    let revision = if !errors && !edits.is_empty() {
        app.image_styles
            .update(
                principal,
                bucket,
                &Update {
                    revision,
                    policy: None,
                    edits,
                },
                Some(&digest),
                !apply,
            )
            .await
            .map_err(validation_error)?
            .revision
    } else {
        revision
    };
    Ok(Imported {
        revision,
        applied: apply,
        rows,
    })
}
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImageExport)]
pub(crate) struct Export {
    pub revision: Option<i64>,
    pub after: Option<String>,
    pub prefix: Option<String>,
    pub names: Option<Vec<String>>,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[schema(as=ImageExportPage)]
pub(crate) struct ExportPage {
    pub text: String,
    pub revision: i64,
    pub next: Option<String>,
}
pub(crate) async fn export_styles(
    app: &App,
    principal: &Principal,
    bucket: Uuid,
    input: &Export,
) -> Result<ExportPage> {
    let selected = input.names.is_some();
    let page = if let Some(names) = &input.names {
        ensure!(
            input.after.is_none() && input.prefix.is_none() && names.len() <= PAGE,
            s3s::s3_error!(InvalidArgument, "invalid export selection")
        );
        let rows = app.image_styles.selected(principal, bucket, names).await?;
        ensure!(
            rows.styles.len() == names.len(),
            s3s::s3_error!(InvalidArgument, "missing or repeated export style")
        );
        rows
    } else {
        app.image_styles
            .page(
                principal,
                bucket,
                input.after.as_deref().unwrap_or(""),
                input.prefix.as_deref().unwrap_or(""),
                PAGE as u16,
            )
            .await?
    };
    ensure!(
        input.revision.is_none_or(|r| r == page.settings.revision),
        Error::OperationAborted
    );
    let next =
        (!selected && page.styles.len() == PAGE).then(|| page.styles.last().unwrap().name.clone());
    Ok(ExportPage {
        text: export(&page.styles),
        revision: page.settings.revision,
        next,
    })
}
#[utoipa::path(post,path="/api/v1/buckets/{bucket}/image-processing/styles/export",operation_id="image_styles_export",params(("bucket"=Uuid,Path)),request_body=Export,responses((status=200,body=ExportPage)))]
pub(super) async fn download(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Json(input): Json<Export>,
) -> Result<Json<ExportPage>, HttpError> {
    Ok(Json(
        export_styles(&app, &actor.principal, bucket, &input).await?,
    ))
}

#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub(super) struct Download {
    revision: Option<i64>,
    prefix: Option<String>,
}
#[utoipa::path(get,path="/api/v1/buckets/{bucket}/image-processing/styles/export",operation_id="image_styles_download",params(("bucket"=Uuid,Path),Download),responses((status=200,body=String,content_type="text/plain")))]
pub(super) async fn stream(
    State(app): State<Arc<App>>,
    Extension(actor): Extension<Identity>,
    Path(bucket): Path<Uuid>,
    Query(input): Query<Download>,
) -> Result<axum::response::Response, HttpError> {
    use axum::{
        body::{Body, Bytes},
        http::header,
        response::IntoResponse,
    };
    let first = export_styles(
        &app,
        &actor.principal,
        bucket,
        &Export {
            revision: input.revision,
            after: None,
            prefix: input.prefix.clone(),
            names: None,
        },
    )
    .await?;
    let revision = first.revision;
    let stream = futures_util::stream::try_unfold((Some(first), None), move |(first, after)| {
        let app = app.clone();
        let principal = actor.principal.clone();
        let prefix = input.prefix.clone();
        async move {
            let page = if let Some(first) = first {
                first
            } else if let Some(after) = after {
                #[cfg(feature = "fault-injection")]
                crate::faults::point("image-export-between-pages").await;
                export_styles(
                    &app,
                    &principal,
                    bucket,
                    &Export {
                        revision: Some(revision),
                        after: Some(after),
                        prefix,
                        names: None,
                    },
                )
                .await?
            } else {
                return Ok::<_, anyhow::Error>(None);
            };
            Ok(Some((Bytes::from(page.text), (None, page.next))))
        }
    });
    Ok((
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"image-styles.txt\"",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}
