use super::*;
use crate::model::node_request::Catalog;

impl Gateway {
    pub(super) async fn create_multipart(
        &self,
        request: S3Request<CreateMultipartUploadInput>,
    ) -> Result<S3Response<CreateMultipartUploadOutput>> {
        gateway::reject_features(&request.headers)?;
        let access = &request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            .access_key;
        let i = request.input;
        let algorithm = i
            .checksum_algorithm
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let checksum_type = crate::upload::multipart::checksum_type(
            algorithm.as_deref(),
            i.checksum_type.as_ref().map(|value| value.as_str()),
        )?;
        let reply = objects::prepared()?
            .create_multipart(
                &self.reads,
                wire::CreateMultipartRequest {
                    access_key: access.clone(),
                    bucket: i.bucket.clone(),
                    key: i.key.clone(),
                    metadata_json: serde_json::to_vec(&gateway::metadata!(i))?,
                    public_read: gateway::canned(i.acl.as_ref())?,
                    algorithm,
                    checksum_type: checksum_type.clone(),
                    ..Default::default()
                },
            )
            .await?;
        let id = reply.upload_id.context("multipart identity missing")?;
        uuid::Uuid::parse_str(&id)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(i.bucket),
            key: Some(i.key),
            upload_id: Some(id),
            checksum_algorithm: i.checksum_algorithm,
            checksum_type: checksum_type.map(ChecksumType::from),
            ..Default::default()
        }))
    }

    pub(super) async fn abort_multipart(
        &self,
        request: S3Request<AbortMultipartUploadInput>,
    ) -> Result<S3Response<AbortMultipartUploadOutput>> {
        let access = &request
            .credentials
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(AccessDenied))?
            .access_key;
        let i = request.input;
        let id = uuid::Uuid::parse_str(&i.upload_id).map_err(|_| s3s::s3_error!(NoSuchUpload))?;
        ensure!(!id.is_nil(), s3s::s3_error!(NoSuchUpload));
        let reply = objects::prepared()?
            .abort_multipart(
                &self.reads,
                wire::AbortMultipartRequest {
                    access_key: access.clone(),
                    bucket: i.bucket,
                    key: i.key,
                    upload_id: id.to_string(),
                    initiated: i
                        .if_match_initiated_time
                        .map(gateway::http_date)
                        .transpose()?,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(reply.upload_id.is_none(), "invalid multipart abort receipt");
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
}

pub(super) fn uploads(
    request: S3Request<ListMultipartUploadsInput>,
) -> Result<S3Response<ListMultipartUploadsOutput>> {
    let i = request.input;
    let result = catalog::result(
        &i.bucket,
        request
            .credentials
            .as_ref()
            .map(|value| value.access_key.as_str()),
        wire::prepare_request_input::Query::ListUploads(wire::ListUploadsQuery {
            prefix: i.prefix.clone(),
            delimiter: i.delimiter.clone(),
            key_marker: i.key_marker.clone(),
            upload_marker: i.upload_id_marker.clone(),
            limit: i.max_uploads.unwrap_or(1000),
        }),
    )?;
    let Catalog::Uploads(result) = result else {
        anyhow::bail!("invalid upload listing reply")
    };
    ensure!(
        result.uploads.len() + result.prefixes.len() <= i.max_uploads.unwrap_or(1000) as usize,
        "upload listing exceeds requested limit"
    );
    crate::upload::listing::output(i, result)
}

pub(super) fn parts(request: S3Request<ListPartsInput>) -> Result<S3Response<ListPartsOutput>> {
    let i = request.input;
    let result = catalog::result(
        &i.bucket,
        request
            .credentials
            .as_ref()
            .map(|value| value.access_key.as_str()),
        wire::prepare_request_input::Query::ListParts(wire::ListPartsQuery {
            key: i.key.clone(),
            upload_id: i.upload_id.clone(),
            marker: i.part_number_marker.unwrap_or(0),
            limit: i.max_parts.unwrap_or(1000),
        }),
    )?;
    let Catalog::Parts {
        rows,
        algorithm,
        checksum_type,
    } = result
    else {
        anyhow::bail!("invalid part listing reply")
    };
    ensure!(
        rows.len() <= i.max_parts.unwrap_or(1000) as usize + 1,
        "part listing exceeds requested limit"
    );
    crate::upload::multipart::listed_parts(i, rows, algorithm, checksum_type)
}
