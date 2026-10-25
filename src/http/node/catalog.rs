use super::*;
use crate::model::node_request::Catalog;
use wire::prepare_request_input::Query;

pub(super) fn object_read(route: &ReadRoute, query: &[String]) -> bool {
    let object = match route {
        ReadRoute::Bucket { key, alias, .. } => {
            !key.is_empty() || alias.as_ref().is_some_and(|(_, key)| !key.is_empty())
        }
        ReadRoute::Host { key, .. } => !key.is_empty(),
    };
    object
        && !query.iter().any(|name| {
            matches!(
                name.as_str(),
                "acl"
                    | "attributes"
                    | "legal-hold"
                    | "retention"
                    | "tagging"
                    | "torrent"
                    | "uploadId"
                    | "uploads"
                    | "restore"
                    | "select"
            )
        })
}

pub(super) fn query(
    method: &Method,
    uri: &hyper::Uri,
    bucket: &str,
    key: &str,
) -> Result<Option<Query>> {
    if !key.is_empty() {
        let axum::extract::Query(values) =
            axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri)?;
        return Ok(
            (method == Method::GET && values.iter().any(|(name, _)| name == "acl")).then(|| {
                Query::ObjectAcl(wire::ObjectAclQuery {
                    key: key.to_owned(),
                })
            }),
        );
    }
    if method == Method::HEAD {
        return Ok(
            (!bucket.is_empty()).then_some(Query::DescribeBucket(wire::DescribeBucketQuery {}))
        );
    }
    if method != Method::GET {
        return Ok(None);
    }
    let axum::extract::Query(values) =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri)?;
    let field = |name: &str| -> Result<Option<String>> {
        let mut matches = values.iter().filter(|(key, _)| key == name);
        let value = matches.next().map(|(_, value)| value.clone());
        ensure!(matches.next().is_none(), s3s::s3_error!(InvalidArgument));
        Ok(value)
    };
    let limit = |name: &str, maximum: i32, zero: bool| -> Result<i32> {
        let value = field(name)?
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|_| s3s::s3_error!(InvalidArgument))?
            .unwrap_or(1000);
        ensure!(
            ((if zero { 0 } else { 1 })..=maximum).contains(&value),
            s3s::s3_error!(InvalidArgument)
        );
        Ok(value)
    };
    Ok(if bucket.is_empty() {
        Some(Query::ListBuckets(wire::ListBucketsQuery {
            prefix: field("prefix")?,
            region: field("bucket-region")?,
            token: field("continuation-token")?,
            limit: limit("max-buckets", 10000, false)?,
        }))
    } else if field("location")?.is_some() {
        Some(Query::DescribeBucket(wire::DescribeBucketQuery {}))
    } else if field("list-type")?.as_deref() == Some("2") {
        Some(Query::ListObjects(wire::ListObjectsQuery {
            prefix: field("prefix")?,
            delimiter: field("delimiter")?,
            after: field("start-after")?,
            token: field("continuation-token")?,
            limit: limit("max-keys", 1000, true)?,
        }))
    } else {
        None
    })
}

fn result(bucket: &str, access: Option<&str>, query: Query) -> Result<Catalog> {
    CURRENT.try_with(|current| -> Result<Catalog> {
        let current = current.borrow();
        let Some(PreparedRequest::Control(prepared)) = current.as_ref() else {
            anyhow::bail!(s3s::s3_error!(InvalidRequest));
        };
        prepared.check()?;
        ensure!(
            prepared.bucket == bucket && Some(prepared.access.as_str()) == access,
            s3s::s3_error!(AccessDenied)
        );
        ensure!(
            prepared.query.as_ref() == Some(&query),
            s3s::s3_error!(InvalidRequest)
        );
        match wire::ReadOutcome::try_from(prepared.reply.outcome)? {
            wire::ReadOutcome::Ready => {}
            wire::ReadOutcome::NoSuchBucket => anyhow::bail!(s3s::s3_error!(NoSuchBucket)),
            _ => anyhow::bail!(s3s::s3_error!(AccessDenied)),
        }
        let reply = serde_json::from_slice(&prepared.reply.catalog_json)?;
        ensure!(
            !matches!(reply, Catalog::InvalidArgument),
            s3s::s3_error!(InvalidArgument)
        );
        Ok(reply)
    })?
}

pub(super) fn describe(bucket: &str, access: Option<&str>) -> Result<()> {
    ensure!(
        matches!(
            result(
                bucket,
                access,
                Query::DescribeBucket(wire::DescribeBucketQuery {})
            )?,
            Catalog::Bucket
        ),
        "invalid bucket reply"
    );
    Ok(())
}

pub(super) fn acl(request: S3Request<GetObjectAclInput>) -> Result<S3Response<GetObjectAclOutput>> {
    let i = request.input;
    let Catalog::Acl { public } = result(
        &i.bucket,
        request
            .credentials
            .as_ref()
            .map(|value| value.access_key.as_str()),
        Query::ObjectAcl(wire::ObjectAclQuery { key: i.key }),
    )?
    else {
        anyhow::bail!("invalid object ACL reply")
    };
    let public = public.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let bucket = CURRENT.try_with(|current| -> Result<_> {
        current
            .borrow()
            .as_ref()
            .context("ACL request missing")?
            .bucket()?
            .context("ACL bucket missing")
    })??;
    Ok(S3Response::new(gateway::object_acl(&bucket, public)))
}

pub(super) fn buckets(
    request: S3Request<ListBucketsInput>,
    region: &str,
) -> Result<S3Response<ListBucketsOutput>> {
    let i = request.input;
    let reply = result(
        "",
        request
            .credentials
            .as_ref()
            .map(|value| value.access_key.as_str()),
        Query::ListBuckets(wire::ListBucketsQuery {
            prefix: i.prefix.clone(),
            region: i.bucket_region.clone(),
            token: i.continuation_token.clone(),
            limit: i.max_buckets.unwrap_or(1000),
        }),
    )?;
    let Catalog::Buckets { rows, next } = reply else {
        anyhow::bail!("invalid bucket listing reply")
    };
    ensure!(
        rows.len() <= i.max_buckets.unwrap_or(1000) as usize,
        "bucket listing exceeds requested limit"
    );
    Ok(S3Response::new(ListBucketsOutput {
        buckets: Some(
            rows.into_iter()
                .map(|row| Bucket {
                    name: Some(row.name),
                    creation_date: Some(gateway::stamp(row.created_at)),
                    bucket_region: Some(region.to_owned()),
                    ..Default::default()
                })
                .collect(),
        ),
        continuation_token: next,
        prefix: i.prefix,
        ..Default::default()
    }))
}

pub(super) fn objects(
    request: S3Request<ListObjectsV2Input>,
) -> Result<S3Response<ListObjectsV2Output>> {
    let i = request.input;
    let reply = result(
        &i.bucket,
        request
            .credentials
            .as_ref()
            .map(|value| value.access_key.as_str()),
        Query::ListObjects(wire::ListObjectsQuery {
            prefix: i.prefix.clone(),
            delimiter: i.delimiter.clone(),
            after: i.start_after.clone(),
            token: i.continuation_token.clone(),
            limit: i.max_keys.unwrap_or(1000),
        }),
    )?;
    let Catalog::Objects {
        rows,
        prefixes,
        next,
    } = reply
    else {
        anyhow::bail!("invalid object listing reply")
    };
    ensure!(
        rows.len() + prefixes.len() <= i.max_keys.unwrap_or(1000) as usize,
        "object listing exceeds requested limit"
    );
    gateway::listed_objects(
        i,
        rows.into_iter()
            .map(|row| (row.key, row.size, row.etag, row.modified_at)),
        prefixes,
        next,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_routes_keep_objects_and_pagination_distinct() -> Result<()> {
        let route = ReadRoute::Bucket {
            name: "bucket".into(),
            key: "path/key".into(),
            alias: None,
        };
        assert!(object_read(
            &route,
            &["response-content-type".into(), "X-Amz-Signature".into()]
        ));
        assert!(object_read(&route, &["unrecognized".into()]));
        assert!(!object_read(&route, &["acl".into()]));
        assert!(matches!(
            query(&Method::GET, &"/bucket/key?acl".parse()?, "bucket", "key")?,
            Some(Query::ObjectAcl(_))
        ));
        assert!(matches!(
            query(&Method::HEAD, &"/bucket".parse()?, "bucket", "")?,
            Some(Query::DescribeBucket(_))
        ));
        let parsed = query(
            &Method::GET,
            &"/bucket?list-type=2&prefix=a%2B&delimiter=%2F&max-keys=0".parse()?,
            "bucket",
            "",
        )?;
        assert!(
            matches!(parsed, Some(Query::ListObjects(ref value)) if value.limit == 0 && value.prefix.as_deref() == Some("a+") && value.delimiter.as_deref() == Some("/"))
        );
        for uri in [
            "/?max-buckets=0",
            "/?max-buckets=10001",
            "/?max-buckets=abc",
            "/?prefix=a&prefix=b",
        ] {
            assert!(query(&Method::GET, &uri.parse()?, "", "").is_err());
        }
        Ok(())
    }
}
