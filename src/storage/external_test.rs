use super::{external::Client, model::Options, s3};
use crate::model::external_test::{
    Artifact, Capability as Cap, CheckState as State, Phase, Plan, Problem, Report, Snapshot,
    TestFile,
};
use anyhow::{Context, Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, Method, Response, StatusCode, header},
};
use bytes::Bytes;
use futures_util::StreamExt;
use std::time::Duration;

const LIMIT: usize = 64 * 1024;
const TIMEOUT: Duration = Duration::from_secs(6);

#[cfg(test)]
mod acl_tests;

pub(crate) async fn execute(plan: Plan, snapshot: &Snapshot) -> Result<Report> {
    plan.report.validate()?;
    ensure!(
        serde_json::to_value(&plan.report.connection)?
            == serde_json::to_value(&snapshot.connection)?,
        "external connection differs"
    );
    ensure!(
        plan.report.phase != Phase::Done,
        "external test already finished"
    );
    let client = snapshot.connection.connect(
        &snapshot.access_key,
        &snapshot.secret_key,
        Options {
            connect_timeout: TIMEOUT,
            request_timeout: TIMEOUT,
            retry_timeout: TIMEOUT,
            max_retries: 0,
        },
    )?;
    let mut probe = Probe {
        client,
        plan,
        uncertain: false,
    };
    probe.plan.report.blocked = None;
    let result = tokio::time::timeout(Duration::from_secs(45), probe.step()).await;
    if !matches!(result, Ok(Ok(()))) && probe.plan.report.blocked.is_none() {
        probe.plan.report.blocked = Some(if probe.uncertain {
            Problem::UncertainResult
        } else {
            Problem::Unavailable
        });
    }
    if probe.plan.report.blocked.is_none() {
        probe.plan.report.phase = probe.plan.report.phase.next();
    }
    probe.plan.report.validate()?;
    Ok(probe.plan.report)
}

struct Probe {
    client: Client,
    plan: Plan,
    uncertain: bool,
}

impl Probe {
    fn key(&self, file: TestFile) -> String {
        file.key(self.plan.id)
    }
    fn full_key(&self, key: &str) -> String {
        let prefix = &self.plan.report.connection.prefix;
        if prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{prefix}/{key}")
        }
    }
    fn payload(&self) -> Bytes {
        self.plan.id.as_bytes().repeat(1024).into()
    }
    fn headers(&self) -> Result<HeaderMap> {
        let mut value = HeaderMap::new();
        value.insert("x-amz-meta-mokyu-test", self.plan.id.to_string().parse()?);
        value.insert(header::CACHE_CONTROL, "no-store".parse()?);
        Ok(value)
    }
    fn blocked(&mut self, problem: Problem) {
        self.plan.report.blocked = Some(problem);
    }
    fn check(&mut self, cap: Cap, state: State, status: Option<u16>) {
        self.plan.report.check(cap, state, status);
    }

    async fn request(
        &mut self,
        cap: Cap,
        method: Method,
        key: Option<&str>,
        query: &[(&str, &str)],
        headers: HeaderMap,
        body: Bytes,
        mutation: bool,
    ) -> Result<Option<Response<Body>>> {
        self.uncertain = mutation;
        let result = self.client.bytes(method, key, query, headers, body).await;
        let reply = match result {
            Ok(reply) => reply,
            Err(error) => {
                if error
                    .downcast_ref::<hyper_util::client::legacy::Error>()
                    .is_some_and(|error| error.is_connect())
                {
                    self.uncertain = false;
                }
                self.check(cap, State::Unknown, None);
                return Err(error);
            }
        };
        let status = reply.status();
        self.check(
            cap,
            match status.as_u16() {
                200..=299 => State::Passed,
                401 | 403 => State::Denied,
                405 | 501 => State::Unsupported,
                _ => State::Failed,
            },
            Some(status.as_u16()),
        );
        if status.is_success() {
            return Ok(Some(reply));
        }
        if mutation && status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED {
            self.check(cap, State::Unknown, Some(status.as_u16()));
            self.blocked(Problem::UncertainResult);
        } else {
            self.uncertain = false;
        }
        Ok(None)
    }

    async fn step(&mut self) -> Result<()> {
        match self.plan.report.phase {
            Phase::Inspect => self.inspect().await,
            Phase::Write => self.write().await,
            Phase::ConditionalWrite => self.conditional_write().await,
            Phase::ReadAcl => self.read_acl().await,
            Phase::WriteAcl => self.write_acl().await,
            Phase::PublicRead => self.public_read().await,
            Phase::RestoreAcl => self.restore_acl().await,
            Phase::StartMultipart => self.start(TestFile::Multipart).await,
            Phase::CompleteMultipart => self.complete().await,
            Phase::StartAbort => self.start(TestFile::Abort).await,
            Phase::Abort => self.abort_test().await,
            Phase::Cleanup => self.cleanup().await,
            Phase::Done => unreachable!(),
        }
    }

    async fn inspect(&mut self) -> Result<()> {
        let prefix = if self.plan.report.connection.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.plan.report.connection.prefix)
        };
        let reply = self
            .request(
                Cap::List,
                Method::GET,
                None,
                &[
                    ("list-type", "2"),
                    ("max-keys", "1"),
                    ("encoding-type", "url"),
                    ("prefix", &prefix),
                ],
                HeaderMap::new(),
                Bytes::new(),
                false,
            )
            .await?;
        if let Some(reply) = reply {
            self.check(Cap::List, State::Unknown, Some(200));
            let body = self.client.bounded(reply.into_body(), LIMIT).await?;
            match listed_key(&body, &prefix) {
                Ok(Some((key, size))) => {
                    self.check(Cap::List, State::Passed, Some(200));
                    if let Some(head) = self
                        .request(
                            Cap::Head,
                            Method::HEAD,
                            Some(&key),
                            &[],
                            HeaderMap::new(),
                            Bytes::new(),
                            false,
                        )
                        .await?
                    {
                        self.version(head.headers());
                    }
                    let mut headers = HeaderMap::new();
                    if size > 0 {
                        headers.insert(header::RANGE, "bytes=0-1023".parse()?);
                    }
                    if let Some(reply) = self
                        .request(
                            Cap::Read,
                            Method::GET,
                            Some(&key),
                            &[],
                            headers,
                            Bytes::new(),
                            false,
                        )
                        .await?
                    {
                        let status = reply.status();
                        self.check(Cap::Read, State::Unknown, Some(status.as_u16()));
                        if size > 0 {
                            self.check(
                                Cap::Range,
                                if status == StatusCode::PARTIAL_CONTENT
                                    && reply.headers().get(header::CONTENT_RANGE).is_some_and(
                                        |value| value.as_bytes().starts_with(b"bytes 0-"),
                                    )
                                {
                                    State::Passed
                                } else {
                                    State::Unsupported
                                },
                                Some(status.as_u16()),
                            );
                        }
                        // A server ignoring Range may stream a large object; only sample the prefix.
                        let mut stream = reply.into_body().into_data_stream();
                        let mut seen = 0;
                        while seen < size.min(1024) {
                            let part = tokio::time::timeout(TIMEOUT, stream.next())
                                .await?
                                .context("external sample truncated")??;
                            seen += part.len() as u64;
                        }
                        self.check(Cap::Read, State::Passed, Some(status.as_u16()));
                    }
                }
                Ok(None) => self.check(Cap::List, State::Passed, Some(200)),
                Err(_) => self.check(Cap::List, State::Failed, Some(200)),
            }
        }
        self.uploads(None).await?;
        Ok(())
    }

    fn version(&mut self, headers: &HeaderMap) {
        if text(headers, "x-amz-version-id").is_some_and(|value| value != "null") {
            self.check(Cap::Version, State::Passed, None);
        }
    }

    fn artifact(&mut self, file: TestFile, headers: &HeaderMap) -> Result<()> {
        let etag = text(headers, "etag").context("external ETag missing")?;
        let version = text(headers, "x-amz-version-id");
        self.version(headers);
        if !self
            .plan
            .report
            .artifacts
            .iter()
            .any(|item| item.file == file && item.upload.is_none() && item.version == version)
        {
            self.plan.report.artifacts.push(Artifact {
                file,
                version,
                upload: None,
                etag: Some(etag),
                cleaned: false,
            });
        }
        self.uncertain = false;
        Ok(())
    }

    async fn owned(&mut self, file: TestFile, expected: &[u8]) -> Result<Option<HeaderMap>> {
        let key = self.key(file);
        let reply = self
            .client
            .bytes(Method::GET, Some(&key), &[], HeaderMap::new(), Bytes::new())
            .await?;
        if reply.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !reply.status().is_success() {
            self.blocked(Problem::Unavailable);
            return Ok(None);
        }
        let headers = reply.headers().clone();
        if text(&headers, "x-amz-meta-mokyu-test").as_deref() != Some(&self.plan.id.to_string()) {
            self.blocked(Problem::IdentityConflict);
            return Ok(None);
        }
        let body = self.client.bounded(reply.into_body(), LIMIT).await?;
        if body != expected {
            self.blocked(Problem::IdentityConflict);
            return Ok(None);
        }
        Ok(Some(headers))
    }

    async fn write(&mut self) -> Result<()> {
        let file = TestFile::Write;
        let key = self.key(file);
        let payload = self.payload();
        if self.plan.recover {
            if let Some(headers) = self.owned(file, &payload).await? {
                self.artifact(file, &headers)?;
                self.check(Cap::Write, State::Passed, Some(200));
                self.check(Cap::Readback, State::Passed, Some(200));
            } else if self.plan.report.blocked.is_none() {
                self.blocked(Problem::UncertainResult);
            }
            return Ok(());
        }
        let existing = self
            .client
            .bytes(
                Method::HEAD,
                Some(&key),
                &[],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if existing.status() != StatusCode::NOT_FOUND {
            self.blocked(Problem::IdentityConflict);
            return Ok(());
        }
        let mut headers = self.headers()?;
        headers.insert(header::IF_NONE_MATCH, "*".parse()?);
        if let Some(reply) = self
            .request(
                Cap::Write,
                Method::PUT,
                Some(&key),
                &[],
                headers,
                payload.clone(),
                true,
            )
            .await?
        {
            self.artifact(file, reply.headers())?;
            match self.owned(file, &payload).await? {
                Some(_) => self.check(Cap::Readback, State::Passed, Some(200)),
                None => self.check(Cap::Readback, State::Failed, None),
            }
            if self.plan.report.blocked == Some(Problem::Unavailable) {
                self.plan.report.blocked = None;
            }
        }
        Ok(())
    }

    async fn acl_artifact(&mut self) -> Result<Option<(Artifact, Bytes)>> {
        let Some(artifact) = self
            .plan
            .report
            .artifacts
            .iter()
            .rev()
            .find(|item| item.file == TestFile::Write && item.upload.is_none() && !item.cleaned)
            .cloned()
        else {
            return Ok(None);
        };
        let key = self.key(artifact.file);
        let query = artifact
            .version
            .as_deref()
            .map(|version| vec![("versionId", version)])
            .unwrap_or_default();
        let reply = self
            .client
            .bytes(
                Method::GET,
                Some(&key),
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        if !reply.status().is_success() {
            self.blocked(Problem::Unavailable);
            return Ok(None);
        }
        if text(reply.headers(), "x-amz-meta-mokyu-test").as_deref()
            != Some(&self.plan.id.to_string())
            || text(reply.headers(), "etag") != artifact.etag
        {
            self.blocked(Problem::IdentityConflict);
            return Ok(None);
        }
        let body = self.client.bounded(reply.into_body(), LIMIT).await?;
        let expected = if self
            .plan
            .report
            .checks
            .get(&Cap::ConditionalWrite)
            .is_some_and(|check| check.state == State::Unsupported)
        {
            Bytes::from_static(b"condition ignored")
        } else {
            self.payload()
        };
        if body != expected {
            self.blocked(Problem::IdentityConflict);
            return Ok(None);
        }
        Ok(Some((artifact, body)))
    }

    async fn acl_request(
        &mut self,
        artifact: &Artifact,
        cap: Cap,
        public: Option<bool>,
    ) -> Result<Option<Bytes>> {
        let key = self.key(artifact.file);
        let mut query = vec![("acl", "")];
        if let Some(version) = artifact.version.as_deref() {
            query.push(("versionId", version));
        }
        let mut headers = HeaderMap::new();
        if let Some(public) = public {
            headers.insert(
                "x-amz-acl",
                if public { "public-read" } else { "private" }.parse()?,
            );
        }
        self.uncertain = public.is_some();
        self.check(cap, State::Unknown, None);
        let reply = self
            .client
            .bytes(
                if public.is_some() {
                    Method::PUT
                } else {
                    Method::GET
                },
                Some(&key),
                &query,
                headers,
                Bytes::new(),
            )
            .await?;
        let status = reply.status().as_u16();
        let body = self.client.bounded(reply.into_body(), LIMIT).await?;
        if (200..=299).contains(&status) {
            self.uncertain = false;
            if public.is_some() && !body.iter().all(u8::is_ascii_whitespace) {
                self.check(cap, State::Failed, Some(status));
                return Ok(None);
            }
            self.check(cap, State::Passed, Some(status));
            return Ok(Some(body));
        }
        let state = if status >= 500 && status != 501 {
            if public.is_some() {
                self.blocked(Problem::UncertainResult);
            }
            State::Unknown
        } else {
            self.uncertain = false;
            match status {
                401 | 403 if public == Some(true) => State::Blocked,
                401 | 403 => State::Denied,
                405 | 501 => State::Unsupported,
                400 if acl_disabled(&body) => State::Unsupported,
                _ => State::Failed,
            }
        };
        self.check(cap, state, Some(status));
        Ok(None)
    }

    async fn acl_value(&mut self, artifact: &Artifact, cap: Cap) -> Result<Option<bool>> {
        let Some(body) = self.acl_request(artifact, cap, None).await? else {
            return Ok(None);
        };
        match super::external::public_read(&body) {
            Ok(public) => Ok(Some(public)),
            Err(_) => {
                self.check(cap, State::Failed, Some(200));
                Ok(None)
            }
        }
    }

    async fn read_acl(&mut self) -> Result<()> {
        if let Some((artifact, _)) = self.acl_artifact().await? {
            self.acl_value(&artifact, Cap::AclRead).await?;
        }
        Ok(())
    }

    async fn write_acl(&mut self) -> Result<()> {
        if let Some((artifact, _)) = self.acl_artifact().await? {
            if self
                .acl_request(&artifact, Cap::AclWrite, Some(false))
                .await?
                .is_some()
                && self.plan.report.passed(Cap::AclRead)
                && self.acl_value(&artifact, Cap::AclWrite).await? == Some(true)
            {
                self.check(Cap::AclWrite, State::Failed, Some(200));
            }
        }
        Ok(())
    }

    async fn anonymous(
        &mut self,
        artifact: &Artifact,
        expected: &[u8],
        public: bool,
    ) -> Result<()> {
        let cap = if public {
            Cap::PublicRead
        } else {
            Cap::PrivateAccess
        };
        self.check(cap, State::Unknown, None);
        let query = artifact
            .version
            .as_deref()
            .map(|version| vec![("versionId", version)])
            .unwrap_or_default();
        let reply = self
            .client
            .anonymous_read(&self.key(artifact.file), &query)
            .await?;
        let status = reply.status().as_u16();
        let state = match status {
            401 | 403 => {
                if public {
                    State::Blocked
                } else {
                    State::Passed
                }
            }
            200 => {
                let body = self.client.bounded(reply.into_body(), LIMIT).await?;
                if body != expected {
                    State::Failed
                } else if public {
                    State::Passed
                } else {
                    State::Blocked
                }
            }
            500..=599 => State::Unknown,
            _ => State::Failed,
        };
        self.check(cap, state, Some(status));
        Ok(())
    }

    async fn public_read(&mut self) -> Result<()> {
        if !self.plan.report.passed(Cap::AclWrite) {
            return Ok(());
        }
        if let Some((artifact, expected)) = self.acl_artifact().await? {
            if self
                .acl_request(&artifact, Cap::PublicRead, Some(true))
                .await?
                .is_some()
            {
                self.anonymous(&artifact, &expected, true).await?;
            }
        }
        Ok(())
    }

    async fn restore_acl(&mut self) -> Result<()> {
        if !self.plan.report.passed(Cap::AclWrite) {
            return Ok(());
        }
        if let Some((artifact, expected)) = self.acl_artifact().await? {
            if self
                .acl_request(&artifact, Cap::PrivateAccess, Some(false))
                .await?
                .is_some()
            {
                self.anonymous(&artifact, &expected, false).await?;
            }
        }
        Ok(())
    }

    async fn conditional_write(&mut self) -> Result<()> {
        let file = TestFile::Write;
        if !self
            .plan
            .report
            .artifacts
            .iter()
            .any(|item| item.file == file)
        {
            return Ok(());
        }
        let key = self.key(file);
        if self.plan.recover {
            if let Some(headers) = self.owned(file, b"condition ignored").await? {
                self.artifact(file, &headers)?;
                self.check(Cap::ConditionalWrite, State::Unsupported, Some(200));
            } else {
                self.blocked(Problem::UncertainResult);
            }
            return Ok(());
        }
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, "\"mokyu-test-no-match\"".parse()?);
        let reply = self
            .client
            .bytes(Method::GET, Some(&key), &[], headers, Bytes::new())
            .await?;
        self.check(
            Cap::ConditionalRead,
            if reply.status() == StatusCode::PRECONDITION_FAILED {
                State::Passed
            } else {
                State::Unsupported
            },
            Some(reply.status().as_u16()),
        );
        let mut headers = self.headers()?;
        headers.insert(header::IF_NONE_MATCH, "*".parse()?);
        self.uncertain = true;
        let reply = self
            .client
            .bytes(
                Method::PUT,
                Some(&key),
                &[],
                headers,
                Bytes::from_static(b"condition ignored"),
            )
            .await?;
        let status = reply.status();
        if status == StatusCode::PRECONDITION_FAILED {
            self.uncertain = false;
            self.check(Cap::ConditionalWrite, State::Passed, Some(412));
        } else if status.is_success() {
            self.artifact(file, reply.headers())?;
            self.check(
                Cap::ConditionalWrite,
                State::Unsupported,
                Some(status.as_u16()),
            );
        } else if status.is_client_error() {
            self.uncertain = false;
            self.check(
                Cap::ConditionalWrite,
                if status == StatusCode::FORBIDDEN {
                    State::Denied
                } else {
                    State::Failed
                },
                Some(status.as_u16()),
            );
        } else {
            self.blocked(Problem::UncertainResult);
        }
        Ok(())
    }

    async fn uploads(&mut self, file: Option<TestFile>) -> Result<Option<Vec<String>>> {
        let key = file
            .map(|file| self.key(file))
            .unwrap_or_else(|| format!(".mokyu-tests/{}/", self.plan.id));
        let prefix = self.full_key(&key);
        let Some(reply) = self
            .request(
                Cap::MultipartList,
                Method::GET,
                None,
                &[("uploads", ""), ("prefix", &prefix), ("max-uploads", "8")],
                HeaderMap::new(),
                Bytes::new(),
                false,
            )
            .await?
        else {
            return Ok(None);
        };
        self.check(Cap::MultipartList, State::Unknown, Some(200));
        let body = self.client.bounded(reply.into_body(), LIMIT).await?;
        let (items, more) = s3::multipart_listing(&body)?;
        ensure!(
            !more && items.len() <= 8 && items.iter().all(|(key, _)| key.starts_with(&prefix)),
            "external upload listing exceeds test scope"
        );
        self.check(Cap::MultipartList, State::Passed, Some(200));
        Ok(Some(
            items
                .into_iter()
                .filter(|(key, _)| file.is_none() || key == &prefix)
                .map(|(_, id)| id)
                .collect(),
        ))
    }

    async fn start(&mut self, file: TestFile) -> Result<()> {
        if !self.plan.report.passed(Cap::MultipartList) {
            return Ok(());
        }
        let key = self.key(file);
        let upload = if self.plan.recover {
            let uploads = self.uploads(Some(file)).await?.unwrap_or_default();
            if uploads.len() != 1 {
                self.blocked(Problem::UncertainResult);
                return Ok(());
            }
            uploads.into_iter().next().unwrap()
        } else {
            let Some(reply) = self
                .request(
                    Cap::MultipartStart,
                    Method::POST,
                    Some(&key),
                    &[("uploads", "")],
                    self.headers()?,
                    Bytes::new(),
                    true,
                )
                .await?
            else {
                return Ok(());
            };
            self.check(Cap::MultipartStart, State::Unknown, Some(200));
            let body = self.client.bounded(reply.into_body(), LIMIT).await?;
            created_upload(&body, &self.full_key(&key))?
        };
        self.plan.report.artifacts.push(Artifact {
            file,
            upload: Some(upload),
            etag: None,
            version: None,
            cleaned: false,
        });
        self.uncertain = false;
        self.check(Cap::MultipartStart, State::Passed, Some(200));
        Ok(())
    }

    async fn complete(&mut self) -> Result<()> {
        let file = TestFile::Multipart;
        let Some(index) = self
            .plan
            .report
            .artifacts
            .iter()
            .position(|item| item.file == file && item.upload.is_some() && !item.cleaned)
        else {
            return Ok(());
        };
        let upload = self.plan.report.artifacts[index].upload.clone().unwrap();
        let key = self.key(file);
        let payload = self.payload();
        if self.plan.recover {
            if let Some(headers) = self.owned(file, &payload).await? {
                self.artifact(file, &headers)?;
                self.plan.report.artifacts[index].cleaned = true;
                self.check(Cap::MultipartComplete, State::Passed, Some(200));
            } else if self.plan.report.blocked.is_none() {
                self.blocked(Problem::UncertainResult);
            }
            return Ok(());
        }
        let Some(reply) = self
            .request(
                Cap::MultipartParts,
                Method::PUT,
                Some(&key),
                &[("uploadId", &upload), ("partNumber", "1")],
                HeaderMap::new(),
                payload.clone(),
                false,
            )
            .await?
        else {
            return Ok(());
        };
        let etag = text(reply.headers(), "etag").context("external part ETag missing")?;
        let Some(reply) = self
            .request(
                Cap::MultipartParts,
                Method::GET,
                Some(&key),
                &[("uploadId", &upload), ("max-parts", "1")],
                HeaderMap::new(),
                Bytes::new(),
                false,
            )
            .await?
        else {
            return Ok(());
        };
        self.check(Cap::MultipartParts, State::Unknown, Some(200));
        let parts = s3::parts_listing(
            &self.client.bounded(reply.into_body(), LIMIT).await?,
            &self.full_key(&key),
            &upload,
            0,
        )?;
        ensure!(
            parts.next.is_none()
                && parts.items.len() == 1
                && parts.items[0].number == 1
                && parts.items[0].etag == etag
                && parts.items[0].size == payload.len() as u64,
            "external part differs"
        );
        self.check(Cap::MultipartParts, State::Passed, Some(200));
        let mut body = Vec::new();
        s3s::xml::Serializer::new(&mut body).element("CompleteMultipartUpload", |xml| {
            xml.element("Part", |xml| {
                xml.content("PartNumber", &1)?;
                xml.content("ETag", &etag)
            })
        })?;
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/xml".parse()?);
        let Some(reply) = self
            .request(
                Cap::MultipartComplete,
                Method::POST,
                Some(&key),
                &[("uploadId", &upload)],
                headers,
                body.into(),
                true,
            )
            .await?
        else {
            return Ok(());
        };
        self.check(Cap::MultipartComplete, State::Unknown, Some(200));
        let body = self.client.bounded(reply.into_body(), LIMIT).await?;
        ensure!(
            s3::complete_response(true, &body)?,
            "external completion failed"
        );
        let headers = self
            .owned(file, &payload)
            .await?
            .context("external completed object missing")?;
        self.artifact(file, &headers)?;
        self.plan.report.artifacts[index].cleaned = true;
        self.check(Cap::MultipartComplete, State::Passed, Some(200));
        Ok(())
    }

    async fn abort_test(&mut self) -> Result<()> {
        if let Some(index) =
            self.plan.report.artifacts.iter().position(|item| {
                item.file == TestFile::Abort && item.upload.is_some() && !item.cleaned
            })
        {
            self.clean(index).await?;
        }
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        for index in 0..self.plan.report.artifacts.len() {
            if !self.plan.report.artifacts[index].cleaned {
                self.clean(index).await?;
            }
            if self.plan.report.blocked.is_some() {
                break;
            }
        }
        Ok(())
    }

    async fn clean(&mut self, index: usize) -> Result<()> {
        let item = self.plan.report.artifacts[index].clone();
        let key = self.key(item.file);
        let mut query = Vec::new();
        if let Some(upload) = &item.upload {
            query.push(("uploadId", upload.as_str()));
        }
        if let Some(version) = &item.version {
            query.push(("versionId", version.as_str()));
        }
        let mut headers = HeaderMap::new();
        if item.upload.is_none() {
            let reply = self
                .client
                .bytes(
                    Method::HEAD,
                    Some(&key),
                    &query,
                    HeaderMap::new(),
                    Bytes::new(),
                )
                .await?;
            if reply.status() == StatusCode::NOT_FOUND {
                self.plan.report.artifacts[index].cleaned = true;
                return Ok(());
            }
            if !reply.status().is_success() {
                self.blocked(Problem::CleanupPending);
                return Ok(());
            }
            if text(reply.headers(), "x-amz-meta-mokyu-test").as_deref()
                != Some(&self.plan.id.to_string())
            {
                self.blocked(Problem::IdentityConflict);
                return Ok(());
            }
            if item.version.is_none() && text(reply.headers(), "x-amz-version-id").is_some() {
                self.blocked(Problem::IdentityConflict);
                return Ok(());
            }
            let etag = text(reply.headers(), "etag").context("external cleanup ETag missing")?;
            headers.insert(header::IF_MATCH, etag.parse()?);
        }
        let reply = self
            .client
            .bytes(Method::DELETE, Some(&key), &query, headers, Bytes::new())
            .await?;
        let status = reply.status();
        let cap = if item.upload.is_some() {
            Cap::MultipartAbort
        } else {
            Cap::Delete
        };
        if status.is_success() || status == StatusCode::NOT_FOUND {
            let verify = self
                .client
                .bytes(
                    if item.upload.is_some() {
                        Method::GET
                    } else {
                        Method::HEAD
                    },
                    Some(&key),
                    &query,
                    HeaderMap::new(),
                    Bytes::new(),
                )
                .await?;
            if verify.status() != StatusCode::NOT_FOUND {
                self.check(cap, State::Unknown, Some(verify.status().as_u16()));
                self.blocked(Problem::CleanupPending);
                return Ok(());
            }
            self.plan.report.artifacts[index].cleaned = true;
            self.check(cap, State::Passed, Some(status.as_u16()));
        } else {
            self.check(
                cap,
                if status == StatusCode::FORBIDDEN {
                    State::Denied
                } else {
                    State::Failed
                },
                Some(status.as_u16()),
            );
            self.blocked(Problem::CleanupPending);
        }
        Ok(())
    }
}

fn acl_disabled(bytes: &[u8]) -> bool {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let mut code = None;
    let result = xml.named_element("Error", |xml| {
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            if name == b"Code" {
                if code.is_some() {
                    return Err(s3s::xml::DeError::DuplicateField);
                }
                code = Some(value);
            }
            Ok(())
        })
    });
    result.is_ok()
        && xml.expect_eof().is_ok()
        && code.as_deref() == Some("AccessControlListNotSupported")
}

fn text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty() && value.len() <= 4096 && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
}

fn created_upload(bytes: &[u8], key: &str) -> Result<String> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let (mut remote_key, mut upload) = (None, None);
    xml.named_element("InitiateMultipartUploadResult", |xml| {
        xml.for_each_element(|xml, name| {
            let value: String = xml.content()?;
            match name {
                b"Key" if remote_key.is_none() => remote_key = Some(value),
                b"UploadId" if upload.is_none() => upload = Some(value),
                b"Key" | b"UploadId" => return Err(s3s::xml::DeError::UnexpectedTagName),
                _ => {}
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    let upload = upload.context("external upload ID missing")?;
    ensure!(
        remote_key.as_deref() == Some(key)
            && !upload.is_empty()
            && upload.len() <= 4096
            && !upload.chars().any(char::is_control),
        "invalid external upload identity"
    );
    Ok(upload)
}

fn listed_key(bytes: &[u8], prefix: &str) -> Result<Option<(String, u64)>> {
    let mut xml = s3s::xml::Deserializer::new(bytes);
    let (mut entry, mut encoding) = (None, None);
    xml.named_element("ListBucketResult", |xml| {
        xml.for_each_element(|xml, name| {
            match name {
                b"Contents" if entry.is_none() => {
                    let (mut key, mut size) = (None, None);
                    xml.for_each_element(|xml, name| {
                        if name == b"Owner" {
                            xml.for_each_element(|xml, _| {
                                let _: String = xml.content()?;
                                Ok(())
                            })?;
                        } else {
                            let value: String = xml.content()?;
                            match name {
                                b"Key" if key.is_none() => key = Some(value),
                                b"Size" if size.is_none() => size = Some(value),
                                b"Key" | b"Size" => {
                                    return Err(s3s::xml::DeError::UnexpectedTagName);
                                }
                                _ => {}
                            }
                        }
                        Ok(())
                    })?;
                    entry = Some((key, size));
                }
                b"Contents" => return Err(s3s::xml::DeError::UnexpectedTagName),
                _ => {
                    let value: String = xml.content()?;
                    if name == b"EncodingType" {
                        encoding = Some(value);
                    }
                }
            }
            Ok(())
        })
    })?;
    xml.expect_eof()?;
    let Some((key, size)) = entry else {
        return Ok(None);
    };
    let key = key.context("external listing key missing")?;
    let key = if encoding.as_deref() == Some("url") {
        crate::http::decode_path(&key)?
    } else {
        key
    };
    ensure!(
        key.len() <= 1024 && key.starts_with(prefix),
        "external listing escaped its prefix"
    );
    Ok(Some((
        key.strip_prefix(prefix).unwrap().to_owned(),
        size.context("external listing size missing")?.parse()?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_acls_are_distinct_from_denied_permissions() {
        assert!(acl_disabled(
            b"<Error><Code>AccessControlListNotSupported</Code><Message>Disabled</Message></Error>"
        ));
        assert!(!acl_disabled(b"<Error><Code>AccessDenied</Code></Error>"));
        assert!(!acl_disabled(
            b"<Error><Code>AccessControlListNotSupported</Code><Code>AccessDenied</Code></Error>"
        ));
        assert!(!acl_disabled(
            b"<Error><Code>AccessControlListNotSupported</Code></Error><extra/>"
        ));
        assert!(!acl_disabled(
            b"<Error><Message>AccessControlListNotSupported</Message></Error>"
        ));
    }

    #[test]
    fn external_listing_preserves_opaque_keys_and_rejects_other_scopes() {
        let xml = b"<ListBucketResult><Contents><Key>imports%2Fa%2F..%2F%2Fb%252F</Key><Size>23</Size></Contents><EncodingType>url</EncodingType></ListBucketResult>";
        assert_eq!(
            listed_key(xml, "imports/").unwrap(),
            Some(("a/..//b%2F".into(), 23))
        );
        assert!(listed_key(xml, "other/").is_err());
        let xml = b"<ListBucketResult><Contents><Key>imports%2Fspace%20%2B+%2520</Key><Size>1</Size></Contents><EncodingType>url</EncodingType></ListBucketResult>";
        assert_eq!(
            listed_key(xml, "imports/").unwrap(),
            Some(("space ++%20".into(), 1))
        );
        assert!(created_upload(b"<InitiateMultipartUploadResult><Key>other</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>", "test").is_err());
        assert_eq!(created_upload(b"<InitiateMultipartUploadResult><Key>test</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>", "test").unwrap(), "id");
    }

    #[tokio::test]
    #[ignore = "requires an isolated external S3 fixture"]
    async fn remote_external_test() -> Result<()> {
        ensure!(
            std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES").as_deref() == Ok("isolated-only"),
            "isolated fixture required"
        );
        let snapshot = Snapshot {
            external_id: uuid::Uuid::new_v4(),
            revision: 1,
            connection: super::super::external::Connection {
                endpoint_url: std::env::var("MOKYU_TEST_EXTERNAL_ENDPOINT")?,
                region: "us-east-1".into(),
                bucket: std::env::var("MOKYU_TEST_EXTERNAL_BUCKET")?,
                prefix: "connection-tests".into(),
                virtual_hosted: false,
                allow_http: true,
            },
            access_key: std::env::var("MOKYU_TEST_EXTERNAL_ACCESS")?,
            secret_key: std::env::var("MOKYU_TEST_EXTERNAL_SECRET")?,
        };
        let id = uuid::Uuid::new_v4();
        let mut report = Report::new(snapshot.connection.clone());
        for _ in 0..9 {
            let result = execute(
                Plan {
                    id,
                    report: report.clone(),
                    recover: false,
                },
                &snapshot,
            )
            .await?;
            report.advance(&result)?;
            ensure!(
                result.blocked.is_none(),
                "external test blocked in {:?}: {:?}",
                result.phase,
                result.blocked
            );
            report = result;
            if report.phase == Phase::Done {
                break;
            }
        }
        ensure!(
            report.phase == Phase::Done && report.artifacts.iter().all(|item| item.cleaned),
            "external test left artifacts"
        );
        for cap in [
            Cap::List,
            Cap::Write,
            Cap::Readback,
            Cap::Delete,
            Cap::ConditionalRead,
            Cap::ConditionalWrite,
            Cap::MultipartList,
            Cap::MultipartStart,
            Cap::MultipartParts,
            Cap::MultipartComplete,
            Cap::MultipartAbort,
        ] {
            ensure!(
                report.passed(cap),
                "external capability {cap:?} failed: {:?}",
                report.checks.get(&cap)
            );
        }
        Ok(())
    }
}
