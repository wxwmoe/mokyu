use super::*;
use futures_util::{StreamExt, TryStreamExt};

impl Gateway {
    async fn open_copy_source(
        &self,
        access: &str,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        restore: bool,
        range: Option<&str>,
    ) -> Result<crate::node::reads::Prepared> {
        let source = self
            .reads
            .open(wire::ReadRequest {
                version_id: version.map(str::to_owned),
                restore_version: restore,
                headers: range
                    .into_iter()
                    .map(|v| wire::ReadHeader {
                        name: "range".into(),
                        value: v.as_bytes().to_vec(),
                    })
                    .collect(),
                route: Some(wire::ReadRoute {
                    route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                        name: bucket.to_owned(),
                        key: key.to_owned(),
                        ..Default::default()
                    })),
                }),
                access_key: access.to_owned(),
                method: wire::ReadMethod::Get as i32,
                surface: wire::ReadSurface::S3 as i32,
                include: vec![
                    wire::ReadInclude::Object as i32,
                    wire::ReadInclude::Protection as i32,
                ],
                ..Default::default()
            })
            .await?;
        match wire::ReadOutcome::try_from(source.reply.outcome)? {
            wire::ReadOutcome::Ready => {}
            wire::ReadOutcome::NoSuchBucket => anyhow::bail!(s3s::s3_error!(NoSuchBucket)),
            wire::ReadOutcome::InvalidRange => anyhow::bail!(s3s::s3_error!(InvalidArgument)),
            wire::ReadOutcome::NoSuchKey
                if version.is_some() && source.reply.delete_marker_us.is_some() =>
            {
                anyhow::bail!(s3s::s3_error!(
                    InvalidRequest,
                    "cannot copy a delete marker"
                ))
            }
            wire::ReadOutcome::NoSuchKey if version.is_some() => {
                anyhow::bail!(s3s::s3_error!(NoSuchVersion))
            }
            wire::ReadOutcome::NoSuchKey => anyhow::bail!(s3s::s3_error!(NoSuchKey)),
            wire::ReadOutcome::Pending => return Err(crate::app::readiness::pending()),
            _ => anyhow::bail!(s3s::s3_error!(AccessDenied)),
        }
        Ok(source)
    }

    pub(super) async fn copy_part(
        &self,
        request: S3Request<UploadPartCopyInput>,
    ) -> Result<S3Response<UploadPartCopyOutput>> {
        gateway::reject_features(&request.headers)?;
        let access = &request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            .access_key;
        let i = &request.input;
        CURRENT.try_with(|value| match value.borrow().as_ref() {
            Some(PreparedRequest::Write(w)) => w.authorize(access, &i.bucket),
            _ => Err(s3s::s3_error!(InvalidRequest).into()),
        })??;
        let (bucket, key, version) = gateway::copy_location(&i.copy_source)?;
        let source = self
            .open_copy_source(
                access,
                bucket,
                key,
                version,
                false,
                i.copy_source_range.as_deref(),
            )
            .await?;
        let object: read::Object = source.material(
            source
                .reply
                .object
                .as_ref()
                .context("copy source missing")?,
        )?;
        let range = gateway::part_copy::check(i, &object.stream())?;
        let length = range.end - range.start;
        let version = source.reply.version_id.clone();
        let stream = self.reads.copy_body(source, range).await?;
        let body = crate::http::object::blob(stream.map_err(std::io::Error::other).boxed());
        let part = self
            .part(gateway::part_copy::part_request(request, length, body))
            .await?;
        Ok(gateway::part_copy::receipt(part.output, version))
    }
    pub(super) async fn copy(
        &self,
        request: S3Request<CopyObjectInput>,
    ) -> Result<S3Response<CopyObjectOutput>> {
        gateway::reject_features(&request.headers)?;
        let prepared = CURRENT
            .try_with(|value| value.borrow_mut().take())?
            .context("copy request missing")?;
        let PreparedRequest::Write(prepared) = prepared else {
            anyhow::bail!(s3s::s3_error!(InvalidRequest));
        };
        let access = &request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            .access_key;
        let input = request.input;
        prepared.authorize(access, &input.bucket)?;
        let (bucket, key, version) = gateway::copy_source(&input)?;
        let restore = version.is_some() && bucket == input.bucket && key == input.key;
        let source = self
            .open_copy_source(access, bucket, key, version, restore, None)
            .await?;
        let object: read::Object = source.material(
            source
                .reply
                .object
                .as_ref()
                .context("copy source missing")?,
        )?;
        gateway::conditions(
            &object.stream(),
            input.copy_source_if_match.as_ref(),
            input.copy_source_if_none_match.as_ref(),
            input.copy_source_if_modified_since.as_ref(),
            input.copy_source_if_unmodified_since.as_ref(),
            false,
        )?;
        let metadata = if input
            .metadata_directive
            .as_ref()
            .is_some_and(|value| value.as_str() == "REPLACE")
        {
            serde_json::to_value(gateway::metadata!(input))?
        } else {
            object.metadata.clone()
        };
        let begin = wire::BeginWriteRequest {
            access_key: access.clone(),
            source_version: version.map(str::to_owned),
            bucket: input.bucket,
            key: input.key,
            metadata_json: serde_json::to_vec(&metadata)?,
            public_read: gateway::canned(input.acl.as_ref())?,
            ..Default::default()
        };
        let condition = |name| -> Result<Option<String>> {
            request
                .headers
                .get(name)
                .map(|value| Ok(value.to_str()?.to_owned()))
                .transpose()
        };
        let source_version = source.reply.version_id.clone();
        let version_id = self
            .writes
            .copy(
                prepared,
                begin,
                source,
                condition("if-match")?,
                condition("if-none-match")?,
            )
            .await?;
        Ok(S3Response::new(CopyObjectOutput {
            version_id,
            copy_source_version_id: source_version,
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(object.etag)),
                last_modified: Some(gateway::stamp(chrono::Utc::now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
