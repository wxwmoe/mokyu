use super::{Backend, queries::node_requests as q};
use crate::{
    app::Bucket,
    cluster::NodeState,
    content::codec::{self, Key},
    model::{cluster::NodeRequests, node_read as read},
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

pub(super) async fn materials(
    db: &mut q::Connection,
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
        let Some((revision, encrypted)) = q::node_read_credential(access)
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
    let Some(bucket) = q::node_read_bucket(route).fetch_optional(&mut *db).await? else {
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
        && let Some(profile) = q::storage_profile(storage).fetch_optional(&mut *db).await?
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

#[async_trait::async_trait]
impl NodeRequests for Backend {
    async fn prepare_request(
        &self,
        node: &NodeState,
        key: &Key,
        input: crate::model::node_request::Prepare,
    ) -> Result<crate::model::node_request::Reply> {
        use crate::model::node_request::{self as request, Catalog, Query};
        input.validate()?;
        let mut tx = q::begin(self).await?;
        q::admit(self, &mut tx, node, key, input.run).await?;
        let (mut context, bucket) = materials(
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
                && !q::node_request_allowed(access, bucket.project_id, bucket.id, action)
                    .fetch_one(&mut *tx)
                    .await?
            {
                context.outcome = read::Outcome::AccessDenied;
            } else {
                let result: Result<Catalog> = async {
                    Ok(match query {
                        Query::Bucket => Catalog::Bucket,
                        Query::Acl { key } => Catalog::Acl {
                            public: q::node_object_acl(
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
                            let mut rows = q::node_catalog_buckets(
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
                                let rows = q::node_upload_page(
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
                            let id = Uuid::parse_str(&upload)
                                .map_err(|_| s3s::s3_error!(NoSuchUpload))?;
                            let upload = q::multipart_upload(
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
                            let rows = q::upload_parts_page(id, marker, limit as i64 + 1)
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
                                    q::node_catalog_objects(bucket.id, &prefix, key, inclusive)
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
                        if error.downcast_ref::<s3s::S3Error>().is_some_and(|error| {
                            error.code() == &s3s::S3ErrorCode::NoSuchUpload
                        }) =>
                    {
                        Catalog::NoSuchUpload
                    }
                    Err(error) => return Err(error),
                });
            }
        }
        q::admit(self, &mut tx, node, key, input.run).await?;
        tx.commit().await?;
        Ok(request::Reply { context, catalog })
    }
}
