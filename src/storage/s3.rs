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
                    assert!(query.contains("partNumber=1") && query.contains("X-Amz-Signature="));
                    let count = requests.fetch_add(1, Ordering::Relaxed);
                    if count == 0 {
                        return axum::response::Response::builder()
                            .status(503)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    let mut body = request.into_body().into_data_stream();
                    let mut length = 0;
                    let mut hash = Md5::new();
                    while let Some(bytes) = body.next().await {
                        let bytes = bytes.unwrap();
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
        let streaming = Streaming {
            signer: Arc::new(signer),
            client,
            options: Options {
                connect_timeout: Duration::from_secs(1),
                request_timeout: Duration::from_secs(5),
                retry_timeout: Duration::from_secs(10),
                max_retries: 1,
            },
        };
        let opened = AtomicUsize::new(0);
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            streaming.part(
                &Path::from("raw/test"),
                "upload /+",
                0,
                8 * 1024 * 1024,
                &digest,
                || {
                    opened.fetch_add(1, Ordering::Relaxed);
                    let bytes = bytes.clone();
                    let first_bytes = first_bytes.clone();
                    Box::pin(async_stream::try_stream! {
                        yield bytes.clone();
                        first_bytes.notified().await;
                        for _ in 1..32 { yield bytes.clone(); }
                    })
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.content_id, "\"stored\"");
        assert_eq!(opened.load(Ordering::Relaxed), 2);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        server.abort();
    }
}

pub(crate) struct Streaming {
    pub signer: Arc<dyn Signer>,
    pub client: reqwest::Client,
    pub options: Options,
}

impl Streaming {
    pub async fn part<'a>(
        &self,
        key: &Path,
        upload: &str,
        number: usize,
        length: u64,
        digest: &[u8; 16],
        mut input: impl FnMut() -> BoxStream<'a, Result<Bytes>>,
    ) -> Result<PartId> {
        let started = tokio::time::Instant::now();
        let checksum = STANDARD.encode(digest);
        let options = SignedUrlOptions::default()
            .with_query([
                ("partNumber", (number + 1).to_string()),
                ("uploadId", upload.to_owned()),
            ])
            .with_signed_header(
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
            let body = futures_util::stream::unfold(receiver, |mut receiver| async {
                receiver
                    .recv()
                    .await
                    .map(|bytes| (Ok::<_, std::io::Error>(bytes), receiver))
            });
            let mut source = input();
            let feed = async move {
                let mut sent = 0u64;
                let mut actual = Md5::new();
                while let Some(bytes) = source.next().await {
                    let bytes = bytes?;
                    sent = sent
                        .checked_add(bytes.len() as u64)
                        .context("S3 part length overflow")?;
                    ensure!(sent <= length, "S3 part source exceeded its length");
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
                ensure!(sent == length, "S3 part source was truncated");
                ensure!(
                    actual.finalize().as_slice() == digest,
                    "S3 part replay checksum mismatch"
                );
                drop(sender);
                Ok(true)
            };
            let send = async {
                self.client
                    .put(url)
                    .header(reqwest::header::CONTENT_LENGTH, length)
                    .header("content-md5", &checksum)
                    .body(reqwest::Body::wrap_stream(body))
                    .send()
                    .await
            };
            let (complete, response) = {
                tokio::pin!(feed, send);
                // A rejected request must release a stalled source before retrying.
                tokio::select! {
                    fed = &mut feed => (fed?, send.await),
                    response = &mut send => {
                        let complete = if response.as_ref().is_ok_and(|reply| reply.status().is_success()) {
                            feed.await?
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
                    error.is_timeout()
                        || error.is_connect()
                        || error.is_request()
                        || error.is_body()
                }
            };
            if let Ok(response) = &response
                && response.status().is_success()
            {
                ensure!(complete, "S3 accepted an incomplete part request");
                return Ok(PartId {
                    content_id: response
                        .headers()
                        .get(reqwest::header::ETAG)
                        .context("S3 UploadPart returned no ETag")?
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
                        anyhow::bail!("S3 UploadPart failed with HTTP {}", response.status())
                    }
                    Err(error) => Err(error.without_url().into()),
                };
            }
            attempt += 1;
            tokio::time::sleep(delay).await;
        }
    }
}
