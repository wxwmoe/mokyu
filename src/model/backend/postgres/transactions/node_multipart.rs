use super::*;

pub(in super::super) async fn node_completion_status(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_complete::Lookup,
) -> Result<(write::Phase, crate::model::node_complete::Summary)> {
    input.validate()?;
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    require_node_run(&mut tx, node, input.run, false).await?;
    let bucket = queries::node_read_bucket(&crate::model::ReadRoute::Bucket {
        name: input.bucket,
        key: input.key.clone(),
        alias: None,
    })
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let authority = Permit::for_action(
        Principal::Service {
            access_key: input.access.clone(),
            revision: input.revision,
        },
        bucket.id,
        Action::Write,
    );
    authority.lock(&mut tx).await?;
    let upload = queries::multipart_upload(input.upload, bucket.id, &input.key, &input.access)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    let (state, _, live) = queries::completion_state(upload.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        matches!(state.as_str(), "completing" | "completed"),
        s3s::s3_error!(NoSuchUpload)
    );
    let record = queries::node_completion(upload.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        record.manifest_hash == input.hash,
        s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
    );
    let (mut summary, result) = queries::node_completion_summary(upload.id, node.node_id)
        .fetch_one(&mut *tx)
        .await?;
    summary.result = result;
    tx.commit().await?;
    Ok((
        if state == "completed" {
            write::Phase::Published
        } else if live {
            write::Phase::Busy
        } else {
            write::Phase::Expired
        },
        summary.0,
    ))
}

pub(in super::super) async fn node_complete_sources(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_complete::Sources,
) -> Result<crate::model::node_complete::SourceBatch> {
    use crate::model::node_complete::{SOURCES, SourceBatch};
    ensure!(
        (0..10000).contains(&input.first)
            && !input.reads.is_empty()
            && input.reads.len() <= SOURCES,
        s3s::s3_error!(InvalidArgument)
    );
    let unique: HashSet<_> = input.reads.iter().collect();
    ensure!(
        unique.len() == input.reads.len() && input.reads.iter().all(|id| !id.is_nil()),
        s3s::s3_error!(InvalidArgument)
    );
    let mut tx = pool.begin().await?;
    let row = node_write_owned(&mut tx, node, key, input.run, input.operation).await?;
    require_node_write(&mut tx, &row).await?;
    let upload = row
        .completion_upload
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
    let record = queries::node_completion(upload).fetch_one(&mut *tx).await?;
    ensure!(
        (input.first as usize) < record.summary.parts,
        s3s::s3_error!(InvalidArgument)
    );
    let count = input
        .reads
        .len()
        .min(record.summary.parts - input.first as usize);
    let streams = queries::node_completion_sources(upload, input.first, count as i64)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(streams.len() == count, Error::SourceUnavailable);
    ensure!(
        input.shared_permits.len() <= 16
            && input.shared_permits.iter().collect::<HashSet<_>>().len()
                == input.shared_permits.len(),
        s3s::s3_error!(InvalidArgument)
    );
    let shared = queries::node_read_shared_permits(node, input.run, &input.shared_permits)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        shared.len() == input.shared_permits.len(),
        Error::NodeExpired
    );
    let mut shared: std::collections::BTreeMap<_, _> = shared
        .into_iter()
        .map(|(id, storage, remaining)| {
            (
                storage,
                read::Grant {
                    r#type: read::GrantType::StorageRead,
                    id,
                    storage: Some(storage),
                    valid_for_ms: remaining as u64,
                },
            )
        })
        .collect();
    ensure!(
        shared.len() == input.shared_permits.len(),
        s3s::s3_error!(InvalidArgument)
    );
    let mut parts = Vec::with_capacity(count);
    for (operation, stream) in input.reads.into_iter().zip(streams) {
        let query = read::Query {
            run: input.run,
            operation,
            route: crate::model::ReadRoute::Bucket {
                name: format!("completion:{}", input.operation),
                key: stream.id.to_string(),
                alias: None,
            },
            access: None,
            head: false,
            web: false,
            mapping: true,
            protection: true,
            policy: false,
            headers: Default::default(),
            overrides: Default::default(),
            cached: input.cached.clone(),
            max_items: 16,
            max_bytes: 128 * 1024,
            body: None,
        };
        let mut reply = read::Reply::new(read::Outcome::Ready);
        let object = read::Object::from_stream(&stream)?;
        reply.object = Some(reply.material(
            node.deployment_id,
            read::MaterialType::Object,
            object.id.to_string(),
            &object,
            &query.cached,
        )?);
        reply.length = stream.size as u64;
        reply.etag = stream.etag.clone();
        let reply =
            protect_node_read(&mut tx, node, key, &query, &stream, reply, &mut shared).await?;
        ensure!(
            reply.outcome != read::Outcome::Pending,
            Error::NodeSourcePending
        );
        ensure!(
            reply.outcome == read::Outcome::Ready,
            Error::SourceUnavailable
        );
        parts.push(reply);
    }
    require_node_write(&mut tx, &row).await?;
    tx.commit().await?;
    let next = input.first + count as i32;
    Ok(SourceBatch {
        first: input.first,
        next: ((next as usize) < record.summary.parts).then_some(next),
        parts,
    })
}

pub(super) async fn prepare_node_completion(
    tx: &mut Transaction<'_, Postgres>,
    input: &write::Begin,
    bucket: Uuid,
    authority: &Permit,
    manifest: &crate::model::node_complete::Manifest,
) -> Result<(
    crate::model::multipart::Upload,
    crate::model::node_complete::Summary,
    bool,
)> {
    use crate::model::node_complete::Conditions;
    let hash = manifest.hash()?;
    let upload = queries::multipart_upload(
        manifest.upload,
        bucket,
        &input.intent.key,
        &input.intent.access,
    )
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
    queries::lock_bucket_uploads(bucket)
        .fetch_one(&mut **tx)
        .await?;
    let (state, previous, live) = queries::completion_state(upload.id)
        .fetch_one(&mut **tx)
        .await?;
    ensure!(
        matches!(state.as_str(), "active" | "completing" | "completed"),
        s3s::s3_error!(NoSuchUpload)
    );
    let upload = queries::multipart_upload(
        manifest.upload,
        bucket,
        &input.intent.key,
        &input.intent.access,
    )
    .fetch_one(&mut **tx)
    .await?;
    let mut summary = if let Some(record) = queries::node_completion(upload.id)
        .fetch_optional(&mut **tx)
        .await?
    {
        ensure!(
            record.manifest_hash == hash,
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        record.summary.0
    } else {
        ensure!(
            state == "active" && previous.is_none(),
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        let numbers: Vec<_> = manifest.parts.iter().map(|part| part.number).collect();
        let selected = queries::node_selected_parts(upload.id, &numbers)
            .fetch_all(&mut **tx)
            .await?;
        let summary = manifest.select(&upload, &selected)?;
        let conditions = Conditions {
            if_match: manifest.if_match.clone(),
            if_none: manifest.if_none.clone(),
            checksums: manifest.checksums.clone(),
        };
        queries::save_node_completion(upload.id, &hash, &conditions, &summary)
            .execute(&mut **tx)
            .await?;
        ensure!(
            queries::freeze_node_completion(upload.id, &numbers)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                == selected.len() as u64,
            Error::OperationAborted
        );
        summary
    };
    summary.result = upload.result.clone();
    if !live && state != "completed" {
        queries::begin_upload_completion(&crate::model::multipart::Completion {
            authority,
            upload: upload.id,
            hash: &hash,
            token: input.operation,
            parts: &[],
            node: None,
        })
        .execute(&mut **tx)
        .await?;
    }
    Ok((upload, summary, live))
}

pub(super) async fn node_request_materials(
    db: &mut PgConnection,
    node: &NodeState,
    key: &Key,
    access: Option<&str>,
    route: Option<&crate::model::ReadRoute>,
    include: &[read::MaterialType],
    cached: &[read::Version],
) -> Result<(read::Reply, Option<Bucket>)> {
    let mut reply = read::Reply::new(read::Outcome::Ready);
    if include.contains(&read::MaterialType::Credential)
        && let Some(access) = access
    {
        let Some((revision, encrypted)) = queries::node_read_credential(access)
            .fetch_optional(&mut *db)
            .await?
        else {
            reply.outcome = read::Outcome::AccessDenied;
            return Ok((reply, None));
        };
        let credential = read::Credential {
            access: access.to_owned(),
            revision,
            secret: codec::unprotect(&encrypted, key, access.as_bytes())?,
        };
        reply.credential = Some(reply.material(
            node.deployment_id,
            read::MaterialType::Credential,
            access.to_owned(),
            &credential,
            cached,
        )?);
    }
    let Some(route) = route.filter(|_| {
        include.contains(&read::MaterialType::Bucket)
            || include.contains(&read::MaterialType::Policy)
    }) else {
        return Ok((reply, None));
    };
    let Some(bucket) = queries::node_read_bucket(route)
        .fetch_optional(&mut *db)
        .await?
    else {
        reply.outcome = read::Outcome::NoSuchBucket;
        return Ok((reply, None));
    };
    if include.contains(&read::MaterialType::Bucket) {
        reply.bucket = Some(reply.material(
            node.deployment_id,
            read::MaterialType::Bucket,
            bucket.id.to_string(),
            &bucket,
            cached,
        )?);
    }
    if include.contains(&read::MaterialType::Policy)
        && let Some(storage) = bucket.storage_id
        && let Some(profile) = queries::storage_profile(storage)
            .fetch_optional(&mut *db)
            .await?
    {
        reply.policy = Some(reply.material(
            node.deployment_id,
            read::MaterialType::Policy,
            storage.to_string(),
            &read::Policy::try_from(&profile)?,
            cached,
        )?);
    }
    Ok((reply, Some(bucket)))
}

pub(in super::super) async fn prepare_node_request(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_request::Prepare,
) -> Result<crate::model::node_request::Reply> {
    use crate::model::node_request::{self as request, Catalog, Query};
    input.validate()?;
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, node, input.run, false).await?;
    ensure!(
        !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    let (mut context, bucket) = node_request_materials(
        &mut tx,
        node,
        key,
        input.access.as_deref(),
        input.route.as_ref(),
        &input.include,
        &input.cached,
    )
    .await?;
    let mut catalog = None;
    if let Some(query) = input
        .query
        .filter(|_| context.outcome == read::Outcome::Ready)
    {
        let access = input
            .access
            .as_deref()
            .context("catalog credential missing")?;
        let action = if matches!(&query, Query::Acl { .. }) {
            "object.read"
        } else {
            "bucket.list"
        };
        if let Some(bucket) = &bucket
            && !queries::node_request_allowed(access, bucket.project_id, bucket.id, action)
                .fetch_one(&mut *tx)
                .await?
        {
            context.outcome = read::Outcome::AccessDenied;
        } else {
            let result: Result<Catalog> = async {
                Ok(match query {
                    Query::Bucket => Catalog::Bucket,
                    Query::Acl { key } => Catalog::Acl {
                        public: queries::node_object_acl(
                            bucket.as_ref().context("catalog bucket missing")?.id,
                            &key,
                        )
                        .fetch_optional(&mut *tx)
                        .await?,
                    },
                    Query::Buckets {
                        prefix,
                        region,
                        expected_region,
                        token,
                        limit,
                    } => {
                        let after = crate::app::listing::bucket_cursor(
                            access,
                            &prefix,
                            region.as_deref(),
                            token.as_deref(),
                            limit as i32,
                        )?;
                        let mut rows = queries::node_catalog_buckets(
                            access,
                            &prefix,
                            &after,
                            region.as_deref(),
                            &expected_region,
                            limit as i64 + 1,
                        )
                        .fetch_all(&mut *tx)
                        .await?;
                        let truncated = rows.len() > limit;
                        rows.truncate(limit);
                        let next = if truncated {
                            Some(crate::app::listing::bucket_token(
                                access,
                                &prefix,
                                region.as_deref(),
                                &rows.last().unwrap().name,
                            )?)
                        } else {
                            None
                        };
                        Catalog::Buckets { rows, next }
                    }
                    Query::Uploads {
                        prefix,
                        delimiter,
                        key_marker,
                        upload_marker,
                        limit,
                    } => {
                        let marker = if key_marker.is_some() {
                            upload_marker
                                .as_deref()
                                .map(Uuid::parse_str)
                                .transpose()
                                .map_err(|_| s3s::s3_error!(InvalidArgument))?
                        } else {
                            None
                        };
                        let bucket = bucket.as_ref().context("catalog bucket missing")?;
                        let mut page = crate::upload::listing::Page::new(
                            prefix.clone(),
                            delimiter,
                            key_marker,
                            marker,
                            limit,
                        )?;
                        while let Some((after, inclusive, id)) = page.position() {
                            let rows = queries::node_upload_page(
                                bucket.id, access, &prefix, after, inclusive, id,
                            )
                            .fetch_all(&mut *tx)
                            .await?;
                            page.append(rows)?;
                        }
                        Catalog::Uploads(page.finish())
                    }
                    Query::Parts {
                        key,
                        upload,
                        marker,
                        limit,
                    } => {
                        let id =
                            Uuid::parse_str(&upload).map_err(|_| s3s::s3_error!(NoSuchUpload))?;
                        let upload = queries::multipart_upload(
                            id,
                            bucket.as_ref().context("catalog bucket missing")?.id,
                            &key,
                            access,
                        )
                        .fetch_optional(&mut *tx)
                        .await?
                        .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
                        ensure!(
                            matches!(upload.state.as_str(), "active" | "completing"),
                            s3s::s3_error!(NoSuchUpload)
                        );
                        let rows = queries::upload_parts_page(id, marker, limit as i64 + 1)
                            .fetch_all(&mut *tx)
                            .await?;
                        Catalog::Parts {
                            rows,
                            algorithm: upload.checksum_algorithm,
                            checksum_type: upload.checksum_type,
                        }
                    }
                    Query::Objects {
                        prefix,
                        delimiter,
                        after,
                        token,
                        limit,
                    } => {
                        let bucket = bucket.as_ref().context("catalog bucket missing")?;
                        let mut page = crate::app::listing::Page::new(
                            bucket.id,
                            &prefix,
                            &delimiter,
                            after.as_deref(),
                            token.as_deref(),
                            limit,
                        )?;
                        while let Some((key, inclusive)) = page.position() {
                            let rows =
                                queries::node_catalog_objects(bucket.id, &prefix, key, inclusive)
                                    .fetch_all(&mut *tx)
                                    .await?;
                            page.append(rows, |row| &row.key)?;
                        }
                        let list = page.finish();
                        Catalog::Objects {
                            rows: list.objects,
                            prefixes: list.prefixes,
                            next: list.next,
                        }
                    }
                })
            }
            .await;
            catalog = Some(match result {
                Ok(value) => value,
                Err(error)
                    if error.downcast_ref::<s3s::S3Error>().is_some_and(|error| {
                        error.code() == &s3s::S3ErrorCode::InvalidArgument
                    }) =>
                {
                    Catalog::InvalidArgument
                }
                Err(error)
                    if error
                        .downcast_ref::<s3s::S3Error>()
                        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::NoSuchUpload) =>
                {
                    Catalog::NoSuchUpload
                }
                Err(error) => return Err(error),
            });
        }
    }
    require_node_run(&mut tx, node, input.run, false).await?;
    tx.commit().await?;
    Ok(request::Reply { context, catalog })
}

pub(in super::super) async fn node_multipart_operation(
    pool: &PgPool,
    node: &NodeState,
    key: &Key,
    input: crate::model::node_multipart::Request,
    max_active: Option<i64>,
) -> Result<crate::model::node_multipart::Outcome> {
    use crate::model::node_multipart::{Operation, Outcome};
    let hash = input.validate()?;
    let mut tx = pool.begin().await?;
    authenticate_rpc_node(&mut tx, node, key, false).await?;
    ensure!(
        matches!(node.role.as_str(), "data" | "node"),
        Error::NodeAuthentication
    );
    let session = require_node_run(&mut tx, node, input.run, true).await?;
    queries::lock_staging(format!(
        "mokyu:node-multipart:{}:{}",
        node.node_id, input.operation
    ))
    .execute(&mut *tx)
    .await?;
    if let Some((previous, generation, outcome)) =
        queries::node_multipart_operation(node.node_id, input.operation)
            .fetch_optional(&mut *tx)
            .await?
    {
        ensure!(previous == hash, Error::NodeOperationConflict);
        ensure!(generation == node.credential_generation, Error::NodeExpired);
        tx.commit().await?;
        return Ok(outcome.0);
    }
    let issued = write::operation_time(input.operation)?;
    let now = queries::node_clock().fetch_one(&mut *tx).await?;
    ensure!(
        issued <= now + chrono::Duration::seconds(30)
            && issued >= now - chrono::Duration::minutes(15),
        Error::NodeOperationExpired
    );
    ensure!(
        !session.draining && queries::node_read_admission().fetch_one(&mut *tx).await?,
        Error::NodeInactive
    );
    let intent = input.intent;
    let route = crate::model::ReadRoute::Bucket {
        name: intent.bucket,
        key: String::new(),
        alias: None,
    };
    let bucket = queries::node_read_bucket(&route)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let mut authority = Permit::for_action(
        Principal::Service {
            access_key: intent.access.clone(),
            revision: intent.credential_revision,
        },
        bucket.id,
        Action::Write,
    );
    let outcome = match intent.operation {
        Operation::Create {
            metadata,
            public,
            algorithm,
            checksum_type,
        } => {
            if public {
                authority.add(bucket.id, Action::Acl);
            }
            let (upload, _) = create_multipart_in(
                &mut tx,
                MultipartCreate {
                    authority: &authority,
                    bucket: bucket.id,
                    key: &intent.key,
                    access: &intent.access,
                    metadata,
                    public,
                    algorithm,
                    checksum_type,
                    web: None,
                    max_active,
                },
            )
            .await?;
            Outcome::Created { upload }
        }
        Operation::Abort { upload, initiated } => {
            authority.lock(&mut tx).await?;
            let owned = queries::multipart_upload(upload, bucket.id, &intent.key, &intent.access)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| s3s::s3_error!(NoSuchUpload))?;
            if let Some(value) = initiated {
                ensure!(
                    s3s::dto::Timestamp::parse(s3s::dto::TimestampFormat::HttpDate, &value)?
                        == crate::http::gateway::stamp(owned.created_at),
                    s3s::s3_error!(PreconditionFailed)
                );
            }
            abort_multipart_in(&mut tx, &authority, upload, false).await?;
            Outcome::Aborted
        }
    };
    queries::save_node_multipart_operation(node, input.operation, &hash, &outcome)
        .execute(&mut *tx)
        .await?;
    queries::expire_node_multipart_operations(node.node_id)
        .execute(&mut *tx)
        .await?;
    require_node_run(&mut tx, node, input.run, true).await?;
    tx.commit().await?;
    Ok(outcome)
}
