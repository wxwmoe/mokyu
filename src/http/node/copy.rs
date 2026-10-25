use super::*;

impl Gateway {
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
        let (bucket, key) = gateway::copy_source(&input)?;
        let source = self
            .reads
            .open(wire::ReadRequest {
                route: Some(wire::ReadRoute {
                    route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                        name: bucket.to_owned(),
                        key: key.to_owned(),
                        ..Default::default()
                    })),
                }),
                access_key: access.clone(),
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
            wire::ReadOutcome::NoSuchKey => anyhow::bail!(s3s::s3_error!(NoSuchKey)),
            wire::ReadOutcome::Pending => return Err(crate::app::readiness::pending()),
            _ => anyhow::bail!(s3s::s3_error!(AccessDenied)),
        }
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
        self.writes
            .copy(
                prepared,
                begin,
                source,
                condition("if-match")?,
                condition("if-none-match")?,
            )
            .await?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(object.etag)),
                last_modified: Some(gateway::stamp(chrono::Utc::now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
