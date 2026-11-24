use super::*;

pub(crate) fn span(size: i64, header: Option<&str>) -> Result<std::ops::Range<u64>> {
    let size = u64::try_from(size)?;
    let range = if let Some(value) = header {
        let bounds = value.strip_prefix("bytes=").and_then(|s| s.split_once('-'));
        let parse = |s: &str| {
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            s.parse::<u64>().ok()
        };
        let (first, last) = bounds
            .and_then(|(a, b)| Some((parse(a)?, parse(b)?)))
            .filter(|(first, last)| first <= last && *last < size)
            .ok_or_else(|| s3_error!(InvalidArgument, "invalid copy source range"))?;
        anyhow::ensure!(
            size > 5 * 1024 * 1024,
            s3_error!(InvalidRequest, "source too small for ranged copy")
        );
        first..last + 1
    } else {
        0..size
    };
    anyhow::ensure!(
        range.end - range.start <= 5 * 1024 * 1024 * 1024,
        s3_error!(EntityTooLarge)
    );
    Ok(range)
}

pub(crate) fn part_request(
    request: S3Request<UploadPartCopyInput>,
    size: u64,
    body: StreamingBlob,
) -> S3Request<UploadPartInput> {
    let mut request = request.map_input(|i| UploadPartInput {
        bucket: i.bucket,
        key: i.key,
        upload_id: i.upload_id,
        part_number: i.part_number,
        content_length: Some(size as i64),
        body: Some(body),
        ..Default::default()
    });
    // The original request signs an empty body. The new body is the selected source range.
    request.headers.clear();
    request.trailing_headers = None;
    request
}

pub(crate) fn receipt(
    out: UploadPartOutput,
    version: Option<String>,
) -> S3Response<UploadPartCopyOutput> {
    S3Response::new(UploadPartCopyOutput {
        copy_source_version_id: version,
        copy_part_result: Some(CopyPartResult {
            last_modified: Some(stamp(Utc::now())),
            e_tag: out.e_tag,
            checksum_crc32: out.checksum_crc32,
            checksum_crc32c: out.checksum_crc32c,
            checksum_crc64nvme: out.checksum_crc64nvme,
            checksum_md5: out.checksum_md5,
            checksum_sha1: out.checksum_sha1,
            checksum_sha256: out.checksum_sha256,
            checksum_sha512: out.checksum_sha512,
            checksum_xxhash3: out.checksum_xxhash3,
            checksum_xxhash64: out.checksum_xxhash64,
            checksum_xxhash128: out.checksum_xxhash128,
        }),
        ..Default::default()
    })
}

pub(crate) fn check(
    i: &UploadPartCopyInput,
    object: &StoredStream,
) -> Result<std::ops::Range<u64>> {
    conditions(
        object,
        i.copy_source_if_match.as_ref(),
        i.copy_source_if_none_match.as_ref(),
        i.copy_source_if_modified_since.as_ref(),
        i.copy_source_if_unmodified_since.as_ref(),
        false,
    )?;
    span(object.size, i.copy_source_range.as_deref())
}

impl Gateway {
    pub(super) async fn copy_part(
        &self,
        request: S3Request<UploadPartCopyInput>,
    ) -> Result<S3Response<UploadPartCopyOutput>> {
        reject_features(&request.headers)?;
        let i = &request.input;
        let access = request.credentials.as_ref().map(|c| c.access_key.as_str());
        let target = self.0.bucket(&i.bucket, true).await?;
        self.0.authorize(access, target.id, Action::Write).await?;
        let (bucket, key, version) = super::copy_location(&i.copy_source)?;
        let bucket = self.0.bucket(bucket, false).await?;
        let action = if version.is_some() {
            Action::ReadVersion
        } else {
            Action::Read
        };
        let authority = self.0.authorize(access, bucket.id, action).await?;
        let (source, pin, version) = if version.is_some() || bucket.version_mode != 0 {
            let (stream, pin, id) = self
                .0
                .history_read(bucket.id, key, version, &authority)
                .await
                .map_err(|error| {
                    if error
                        .downcast_ref::<s3s::S3Error>()
                        .is_some_and(|s| s.code() == &s3s::S3ErrorCode::MethodNotAllowed)
                    {
                        anyhow::Error::from(s3_error!(
                            InvalidRequest,
                            "cannot copy a delete marker"
                        ))
                    } else {
                        error
                    }
                })?
                .ok_or_else(|| s3_error!(NoSuchKey))?;
            (stream, pin, Some(id))
        } else {
            let (stream, pin) = self.0.current(bucket.id, key).await?;
            (stream, pin, None)
        };
        let range = check(i, &source)?;
        let length = range.end - range.start;
        let app = self.0.clone();
        let stream = async_stream::try_stream! {
            let mut chunks = app.checked_range(&source, range);
            while let Some(chunk) = chunks.next().await {
                yield chunk.map_err(std::io::Error::other)?;
            }
        };
        let body = crate::http::object::blob(Box::pin(stream));
        let part = pin
            .with_read_lease(
                self.0
                    .upload_part(part_request(request, length, body), None),
            )
            .await?;
        Ok(receipt(part.output, version))
    }
}

#[cfg(test)]
mod tests {
    use super::span;
    #[test]
    fn copy_ranges_are_closed_and_bounded() {
        let large = 8 << 20;
        assert_eq!(span(0, None).unwrap(), 0..0);
        assert_eq!(span(large, Some("bytes=5-8")).unwrap(), 5..9);
        for value in [
            "bytes=1-",
            "bytes=-2",
            "bytes=9-8",
            "bytes=+1-2",
            "bytes=0-8388608",
            "bytes=0-18446744073709551615",
        ] {
            assert!(span(large, Some(value)).is_err());
        }
        assert!(span(5 << 20, Some("bytes=0-0")).is_err());
        assert!(span((5i64 << 30) + 1, None).is_err());
    }
}
