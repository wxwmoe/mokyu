use super::*;

pub(super) fn prepared() -> Result<crate::node::request::Request> {
    let value = CURRENT
        .try_with(|value| value.borrow_mut().take())?
        .context("object request missing")?;
    let PreparedRequest::Control(value) = value else {
        anyhow::bail!(s3s::s3_error!(InvalidRequest));
    };
    Ok(value)
}

impl Gateway {
    pub(super) async fn delete(
        &self,
        request: S3Request<DeleteObjectInput>,
    ) -> Result<S3Response<DeleteObjectOutput>> {
        let i = request.input;
        ensure!(i.version_id.is_none(), s3s::s3_error!(NotImplemented));
        let access = request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        prepared()?
            .change(
                &self.reads,
                &access.access_key,
                &i.bucket,
                vec![i.key],
                None,
            )
            .await?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    pub(super) async fn delete_batch(
        &self,
        request: S3Request<DeleteObjectsInput>,
    ) -> Result<S3Response<DeleteObjectsOutput>> {
        let i = request.input;
        ensure!(
            i.delete.objects.len() <= 1000,
            s3s::s3_error!(InvalidArgument)
        );
        let access = request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let mut keys = Vec::new();
        let mut errors = Vec::new();
        for object in i.delete.objects {
            if object.version_id.is_some()
                || object.e_tag.is_some()
                || object.size.is_some()
                || object.last_modified_time.is_some()
            {
                errors.push(Error {
                    key: Some(object.key),
                    code: Some("NotImplemented".into()),
                    ..Default::default()
                });
            } else {
                keys.push(object.key);
            }
        }
        prepared()?
            .change(
                &self.reads,
                &access.access_key,
                &i.bucket,
                keys.clone(),
                None,
            )
            .await?;
        let deleted = if i.delete.quiet == Some(true) {
            vec![]
        } else {
            keys.into_iter()
                .map(|key| DeletedObject {
                    key: Some(key),
                    ..Default::default()
                })
                .collect()
        };
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        }))
    }

    pub(super) async fn set_acl(
        &self,
        request: S3Request<PutObjectAclInput>,
    ) -> Result<S3Response<PutObjectAclOutput>> {
        gateway::reject_features(&request.headers)?;
        let i = request.input;
        ensure!(
            i.access_control_policy.is_none(),
            s3s::s3_error!(NotImplemented, "use private or public-read canned ACL")
        );
        let access = request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
        let public = gateway::canned(i.acl.as_ref())?;
        prepared()?
            .change(
                &self.reads,
                &access.access_key,
                &i.bucket,
                vec![i.key],
                Some(public),
            )
            .await?;
        Ok(S3Response::new(PutObjectAclOutput::default()))
    }
}
