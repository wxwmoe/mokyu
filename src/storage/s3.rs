use super::model::Options;
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use md5::{Digest, Md5};
use object_store::{
    client::{HttpClient, HttpConnector},
    multipart::PartId,
    path::Path,
    signer::{SignedUrlOptions, Signer},
};
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone)]
pub(super) struct Connector(pub reqwest::Client);

impl HttpConnector for Connector {
    fn connect(&self, _: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn uploads_before_source_finishes_and_replays_retryable_parts() {
        replay_upload(true, false, false).await;
    }

    #[tokio::test]
    async fn raw_put_streams_and_replays_after_an_early_error() {
        replay_upload(false, false, false).await;
    }

    #[tokio::test]
    async fn raw_put_stops_when_the_source_makes_no_progress() {
        replay_upload(false, true, false).await;
    }

    #[tokio::test]
    async fn early_success_does_not_hide_an_incomplete_or_stalled_upload() {
        replay_upload(false, true, true).await;
    }

    async fn replay_upload(multipart: bool, stalled: bool, reply_early: bool) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let first_bytes = Arc::new(tokio::sync::Notify::new());
        let requests = Arc::new(AtomicUsize::new(0));
        let bytes = Bytes::from(vec![77; 256 * 1024]);
        let mut hash = Md5::new();
        for _ in 0..32 {
            hash.update(&bytes);
        }
        let digest: [u8; 16] = hash.finalize().into();
        let router = axum::Router::new().fallback(axum::routing::put({
            let requests = requests.clone();
            let first_bytes = first_bytes.clone();
            move |request: axum::extract::Request| {
                let requests = requests.clone();
                let first_bytes = first_bytes.clone();
                async move {
                    assert_eq!(request.headers()["content-md5"], STANDARD.encode(digest));
                    let query = request.uri().query().unwrap();
                    assert_eq!(query.contains("partNumber=1"), multipart);
                    assert!(query.contains("X-Amz-Signature="));
                    let count = requests.fetch_add(1, Ordering::Relaxed);
                    if count == 0 {
                        return axum::response::Response::builder()
                            .status(503)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    if reply_early {
                        return axum::response::Response::builder()
                            .status(200)
                            .header("etag", "\"early\"")
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    let mut body = request.into_body().into_data_stream();
                    let mut length = 0;
                    let mut hash = Md5::new();
                    while let Some(bytes) = body.next().await {
                        let Ok(bytes) = bytes else {
                            assert!(stalled);
                            return axum::response::Response::builder()
                                .status(500)
                                .body(axum::body::Body::empty())
                                .unwrap();
                        };
                        length += bytes.len();
                        hash.update(&bytes);
                        first_bytes.notify_one();
                    }
                    assert_eq!(length, 8 * 1024 * 1024);
                    assert_eq!(hash.finalize().as_slice(), digest);
                    axum::response::Response::builder()
                        .status(200)
                        .header("etag", "\"stored\"")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let signer = object_store::aws::AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name("bucket")
            .with_access_key_id("access")
            .with_secret_access_key("secret")
            .with_http_connector(Connector(client.clone()))
            .build()
            .unwrap();
        let signer = Arc::new(signer);
        let streaming = Streaming {
            statistics: None,
            signer: signer.clone(),
            bucket_signer: signer,
            prefix: Path::from(""),
            client,
            raw_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
            options: Options {
                connect_timeout: Duration::from_secs(1),
                request_timeout: if multipart {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(500)
                },
                retry_timeout: Duration::from_secs(10),
                max_retries: 1,
            },
        };
        let opened = AtomicUsize::new(0);
        let input = || {
            opened.fetch_add(1, Ordering::Relaxed);
            let bytes = bytes.clone();
            let first_bytes = first_bytes.clone();
            Box::pin(async_stream::try_stream! {
                yield bytes.clone();
                first_bytes.notified().await;
                for _ in 1..32 {
                    if !multipart { tokio::time::sleep(Duration::from_millis(if stalled { 1500 } else { 40 })).await; }
                    yield bytes.clone();
                }
            }) as BoxStream<'_, Result<Bytes>>
        };
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            let path = Path::from("raw/test");
            if multipart {
                let reply = streaming
                    .part(&path, "upload /+", 0, 8 * 1024 * 1024, &digest, input)
                    .await?;
                assert_eq!(reply.content_id, "\"stored\"");
            } else {
                streaming
                    .put(&path, 8 * 1024 * 1024, &digest, input)
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .unwrap();
        if stalled {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        if !multipart && !stalled {
            assert!(started.elapsed() > streaming.options.request_timeout);
        }
        assert_eq!(opened.load(Ordering::Relaxed), 2);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        server.abort();
    }
}

pub(crate) struct Streaming {
    pub(crate) statistics: Option<Arc<crate::status::stats::Counters>>,
    pub signer: Arc<dyn Signer>,
    pub bucket_signer: Arc<dyn Signer>,
    pub prefix: Path,
    pub client: reqwest::Client,
    pub raw_client: reqwest::Client,
    pub options: Options,
}

impl Streaming {
    pub(crate) async fn pending_multipart(&self, key: &Path) -> Result<(Vec<String>, bool)> {
        let key = if self.prefix.as_ref().is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        };
        let options = SignedUrlOptions::default().with_query([
            ("uploads", String::new()),
            ("prefix", key.clone()),
            ("max-uploads", "128".into()),
        ]);
        let url = self
            .bucket_signer
            .signed_url_opts(
                reqwest::Method::GET,
                &Path::from(""),
                Duration::from_secs(3600),
                &options,
            )
            .await?;
        let reply = self
            .client
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        ensure!(
            reply.status().is_success(),
            "S3 multipart listing failed: {}",
            reply.status()
        );
        let mut stream = reply.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(part) = stream.next().await {
            let part = part.map_err(reqwest::Error::without_url)?;
            ensure!(
                bytes.len() + part.len() <= 1024 * 1024,
                "S3 multipart listing is too large"
            );
            bytes.extend_from_slice(&part);
        }
        let (uploads, more) = multipart_listing(&bytes)?;
        let ids: Vec<_> = uploads
            .into_iter()
            .filter_map(|(name, id)| (name == key).then_some(id))
            .collect();
        ensure!(
            !more || !ids.is_empty(),
            "S3 multipart listing cannot advance"
        );
        Ok((ids, more))
    }

    pub async fn part<'a>(
        &self,
        key: &Path,
        upload: &str,
        number: usize,
        length: u64,
        digest: &[u8; 16],
        input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
    ) -> Result<PartId> {
        let options = SignedUrlOptions::default().with_query([
            ("partNumber", (number + 1).to_string()),
            ("uploadId", upload.to_owned()),
        ]);
        self.upload(&self.client, key, length, digest, input, options)
            .await
    }

    pub(crate) async fn put<'a>(
        &self,
        key: &Path,
        length: u64,
        digest: &[u8; 16],
        input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
    ) -> Result<()> {
        self.upload(
            &self.raw_client,
            key,
            length,
            digest,
            input,
            SignedUrlOptions::default(),
        )
        .await?;
        Ok(())
    }

    async fn upload<'a>(
        &self,
        client: &reqwest::Client,
        key: &Path,
        length: u64,
        digest: &[u8; 16],
        input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
        options: SignedUrlOptions,
    ) -> Result<PartId> {
        let mut operation = self.statistics.as_ref().map(|counts| counts.begin());
        let result = self
            .send_upload(client, key, length, digest, input, options)
            .await;
        if let Some(operation) = &mut operation {
            if result.is_ok() {
                self.statistics.as_ref().unwrap().bytes(length);
            }
            operation.finish(result.is_err());
        }
        result
    }

    async fn send_upload<'a>(
        &self,
        client: &reqwest::Client,
        key: &Path,
        length: u64,
        digest: &[u8; 16],
        mut input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
        options: SignedUrlOptions,
    ) -> Result<PartId> {
        let started = tokio::time::Instant::now();
        let checksum = STANDARD.encode(digest);
        let options = options.with_signed_header(
            reqwest::header::HeaderName::from_static("content-md5"),
            checksum.parse()?,
        );
        let mut attempt = 0;
        loop {
            let url = self
                .signer
                .signed_url_opts(
                    reqwest::Method::PUT,
                    key,
                    Duration::from_secs(3600),
                    &options,
                )
                .await?;
            let (sender, receiver) = tokio::sync::mpsc::channel::<Bytes>(2);
            let (progress, mut activity) = tokio::sync::watch::channel(tokio::time::Instant::now());
            let body = futures_util::stream::unfold(
                (receiver, progress),
                |(mut receiver, progress)| async {
                    let bytes = receiver.recv().await?;
                    progress.send_replace(tokio::time::Instant::now());
                    Some((Ok::<_, std::io::Error>(bytes), (receiver, progress)))
                },
            );
            let mut source = input();
            let feed = async move {
                let mut sent = 0u64;
                let mut actual = Md5::new();
                while let Some(bytes) = source.next().await {
                    let bytes = bytes?;
                    sent = sent
                        .checked_add(bytes.len() as u64)
                        .context("S3 upload length overflow")?;
                    ensure!(sent <= length, "S3 upload source exceeded its length");
                    actual.update(&bytes);
                    for start in (0..bytes.len()).step_by(256 * 1024) {
                        if sender
                            .send(bytes.slice(start..(start + 256 * 1024).min(bytes.len())))
                            .await
                            .is_err()
                        {
                            // The request result determines whether an early response is retryable.
                            return Ok::<_, anyhow::Error>(false);
                        }
                    }
                }
                ensure!(sent == length, "S3 upload source was truncated");
                ensure!(
                    actual.finalize().as_slice() == digest,
                    "S3 upload replay checksum mismatch"
                );
                drop(sender);
                Ok(true)
            };
            let send = async {
                let request = client
                    .put(url)
                    .header(reqwest::header::CONTENT_LENGTH, length)
                    .header("content-md5", &checksum)
                    .body(reqwest::Body::wrap_stream(body))
                    .send();
                tokio::pin!(request);
                let mut ended = false;
                loop {
                    let deadline = *activity.borrow() + self.options.request_timeout;
                    tokio::select! {
                        response = &mut request => return response.map_err(|error| anyhow::Error::from(error.without_url())),
                        changed = activity.changed(), if !ended => { ended = changed.is_err(); },
                        _ = tokio::time::sleep_until(deadline) => return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "S3 upload made no progress").into()),
                    }
                }
            };
            let (complete, response) = {
                tokio::pin!(feed, send);
                // A rejected request must release a stalled source before retrying.
                tokio::select! {
                    fed = &mut feed => (fed?, send.await),
                    response = &mut send => {
                        let complete = if response.as_ref().is_ok_and(|reply| reply.status().is_success()) {
                            tokio::time::timeout(self.options.request_timeout, feed).await??
                        } else {
                            false
                        };
                        (complete, response)
                    }
                }
            };
            let retryable = match &response {
                Ok(response) => {
                    response.status().is_server_error()
                        || matches!(response.status().as_u16(), 408 | 429)
                }
                Err(error) => {
                    error.downcast_ref::<reqwest::Error>().is_some_and(|error| {
                        error.is_timeout()
                            || error.is_connect()
                            || error.is_request()
                            || error.is_body()
                    }) || error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
                }
            };
            if let Ok(response) = &response
                && response.status().is_success()
            {
                ensure!(complete, "S3 accepted an incomplete upload request");
                return Ok(PartId {
                    content_id: response
                        .headers()
                        .get(reqwest::header::ETAG)
                        .context("S3 upload returned no ETag")?
                        .to_str()?
                        .to_owned(),
                });
            }
            let delay = Duration::from_millis(100u64.saturating_mul(1 << (attempt + 1).min(5)));
            if !retryable
                || attempt >= self.options.max_retries
                || started.elapsed().saturating_add(delay) >= self.options.retry_timeout
            {
                return match response {
                    Ok(response) => {
                        anyhow::bail!("S3 upload failed with HTTP {}", response.status())
                    }
                    Err(error) => Err(error),
                };
            }
            attempt += 1;
            tokio::time::sleep(delay).await;
        }
    }
}

fn multipart_listing(bytes: &[u8]) -> Result<(Vec<(String, String)>, bool)> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let mut uploads = Vec::new();
    let mut more = String::new();
    xml.named_element("ListMultipartUploadsResult", |xml| {
        xml.for_each_element(|xml, name| {
            if name == b"Upload" {
                if uploads.len() >= 128 {
                    return Err(s3s::xml::DeError::UnexpectedTagName);
                }
                let mut key = String::new();
                let mut id = String::new();
                xml.for_each_element(|xml, name| {
                    if matches!(name, b"Owner" | b"Initiator") {
                        xml.for_each_element(|xml, _| {
                            let _: String = xml.content()?;
                            Ok(())
                        })
                    } else {
                        let value: String = xml.content()?;
                        if name == b"Key" {
                            key = value;
                        } else if name == b"UploadId" {
                            id = value;
                        }
                        Ok(())
                    }
                })?;
                uploads.push((key, id));
            } else if name == b"CommonPrefixes" {
                xml.for_each_element(|xml, _| {
                    let _: String = xml.content()?;
                    Ok(())
                })?;
            } else {
                let value: String = xml.content()?;
                if name == b"IsTruncated" {
                    more = value;
                }
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    ensure!(
        matches!(more.as_str(), "" | "false" | "true")
            && uploads.iter().all(|(key, id)| !key.is_empty()
                && !id.is_empty()
                && id.len() <= 4096
                && !id.contains('\0')),
        "invalid S3 multipart listing"
    );
    Ok((uploads, more == "true"))
}

impl Streaming {
    pub(crate) async fn complete(
        &self,
        key: &Path,
        upload: &str,
        parts: &[(i32, String)],
    ) -> Result<bool> {
        let mut body = Vec::new();
        let mut serializer = s3s::xml::Serializer::new(&mut body);
        serializer.element("CompleteMultipartUpload", |xml| {
            for (number, etag) in parts {
                xml.element("Part", |xml| {
                    xml.content("PartNumber", number)?;
                    xml.content("ETag", etag)
                })?;
            }
            Ok(())
        })?;
        let options = SignedUrlOptions::default().with_query([("uploadId", upload.to_owned())]);
        let url = self
            .signer
            .signed_url_opts(
                reqwest::Method::POST,
                key,
                Duration::from_secs(3600),
                &options,
            )
            .await?;
        let reply = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/xml")
            .body(body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        let status = reply.status();
        let mut stream = reply.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(part) = stream.next().await {
            let part = part.map_err(reqwest::Error::without_url)?;
            ensure!(
                bytes.len() + part.len() <= 64 * 1024,
                "S3 completion response is too large"
            );
            bytes.extend_from_slice(&part);
        }
        complete_response(status.is_success(), &bytes)
    }
}

fn complete_response(success: bool, bytes: &[u8]) -> Result<bool> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let mut completed = false;
    let mut code = None;
    xml.element(|xml, root| {
        completed = root == b"CompleteMultipartUploadResult";
        if !completed && root != b"Error" {
            return Err(s3s::xml::DeError::UnexpectedTagName);
        }
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            if name == b"Code" {
                code = Some(value)
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    if code.as_deref() == Some("InvalidPart") {
        return Ok(false);
    }
    ensure!(
        success && completed && code.is_none(),
        "S3 multipart completion failed: {}",
        code.as_deref().unwrap_or("invalid response")
    );
    Ok(true)
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    #[test]
    fn parses_bounded_native_upload_listings() {
        let (uploads,more)=multipart_listing(b"<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><Upload><Key>raw/a</Key><UploadId>first&amp;one</UploadId><Owner><ID>owner</ID></Owner><Initiator><ID>writer</ID><DisplayName>name</DisplayName></Initiator></Upload></ListMultipartUploadsResult>").unwrap();
        assert!(more);
        assert_eq!(uploads, vec![("raw/a".into(), "first&one".into())]);
        assert!(multipart_listing(b"<Error><Code>AccessDenied</Code></Error>").is_err());
        assert!(multipart_listing(b"<ListMultipartUploadsResult><Upload><Key>a</Key></Upload></ListMultipartUploadsResult>").is_err());
        assert!(multipart_listing(b"<ListMultipartUploadsResult><IsTruncated>unknown</IsTruncated></ListMultipartUploadsResult>").is_err());
        let many = format!(
            "<ListMultipartUploadsResult>{}</ListMultipartUploadsResult>",
            "<Upload><Key>a</Key><UploadId>b</UploadId></Upload>".repeat(129)
        );
        assert!(multipart_listing(many.as_bytes()).is_err());
    }
    #[test]
    fn recognizes_late_errors_and_rejects_invalid_results() {
        assert!(complete_response(true,b"  <CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>").unwrap());
        assert!(!complete_response(true, b"<Error><Code>InvalidPart</Code></Error>").unwrap());
        assert!(complete_response(true, b"<Error><Code>InternalError</Code></Error>").is_err());
        assert!(complete_response(false, b"<CompleteMultipartUploadResult/>").is_err());
        assert!(complete_response(true, b"<CompleteMultipartUploadResult/><Error/>").is_err());
        assert!(complete_response(true, b"<SomethingElse/>").is_err());
    }
}
