use super::*;

#[tokio::test]
#[ignore = "requires an isolated external S3 fixture"]
async fn remote_object_transport() -> Result<()> {
    ensure!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES").as_deref() == Ok("isolated-only"),
        "isolated fixture required"
    );
    #[derive(Deserialize)]
    struct Fixture {
        connection: Connection,
        access: String,
        secret: String,
    }
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(std::env::var(
        "MOKYU_TEST_EXTERNAL_CONFIG",
    )?)?)?;
    let connect = || {
        fixture.connection.connect(
            &fixture.access,
            &fixture.secret,
            Options {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(20),
                retry_timeout: Duration::from_secs(20),
                max_retries: 0,
            },
        )
    };
    let client = connect()?;
    let prefix = format!("transfer-{}/", uuid::Uuid::now_v7());
    let source = format!("{prefix}source +%2F #.bin");
    let target = format!("{prefix}target");
    let copied = format!("{prefix}copied");
    let abandoned = format!("{prefix}abandoned");
    let data = Bytes::from(
        (0..WINDOW * 2 + 19)
            .map(|index| ((index * 71) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let metadata = BTreeMap::from([
        ("content-type".into(), "application/octet-stream".into()),
        ("x-amz-meta-check".into(), "source".into()),
    ]);
    client
        .put(&source, &Condition::Absent, &metadata, None, data.clone())
        .await
        .context("external source PUT")?;
    let object = client
        .head(&source, None)
        .await
        .context("external source HEAD")?
        .context("source missing")?;
    ensure!(
        object.size == data.len() as u64 && object.metadata == metadata,
        "source metadata differs"
    );
    let mut offset = 0;
    while offset < data.len() {
        let length = WINDOW.min(data.len() - offset);
        ensure!(
            client.read(&source, &object, offset as u64, length).await?
                == data.slice(offset..offset + length),
            "source range differs"
        );
        offset += length;
    }
    let mut wrong = object.clone();
    wrong.etag = "\"different-source\"".into();
    ensure!(
        client.read(&source, &wrong, 0, 1).await.is_err(),
        "external server ignored conditional reading"
    );
    ensure!(
        client
            .put(
                &source,
                &Condition::Absent,
                &metadata,
                None,
                Bytes::from_static(b"replacement")
            )
            .await
            .is_err(),
        "external server ignored conditional PUT"
    );
    ensure!(
        client.head(&source, None).await?.as_ref() == Some(&object),
        "conditional PUT changed source"
    );
    let upload = client.start_upload(&target, &metadata, None).await?;
    let (first, first_hash) = client
        .transfer_part(
            &target,
            &upload,
            1,
            &client,
            &source,
            &object,
            0,
            WINDOW as u64,
        )
        .await?;
    ensure!(
        first_hash.as_slice() == Sha256::digest(&data[..WINDOW]).as_slice(),
        "first part digest differs"
    );
    drop(client);
    let client = connect()?;
    let page = client
        .parts(&target, &upload, 0)
        .await?
        .context("resumed upload missing")?;
    ensure!(
        page.items.len() == 1
            && page.items[0].etag == first.etag
            && page.items[0].size == WINDOW as u64
            && page.next.is_none(),
        "resumed part receipt differs"
    );
    let (second, second_hash) = client
        .transfer_part(
            &target,
            &upload,
            2,
            &client,
            &source,
            &object,
            WINDOW as u64,
            (data.len() - WINDOW) as u64,
        )
        .await?;
    ensure!(
        second_hash.as_slice() == Sha256::digest(&data[WINDOW..]).as_slice(),
        "streamed part digest differs"
    );
    let written = client
        .complete_upload(&target, &upload, &[first, second], &Condition::Absent)
        .await?;
    let target_object = client
        .head(&target, written.version.as_deref())
        .await?
        .context("target missing")?;
    ensure!(
        target_object.size == object.size
            && target_object.etag == written.etag
            && target_object.metadata == metadata,
        "target receipt differs"
    );
    offset = 0;
    while offset < data.len() {
        let length = WINDOW.min(data.len() - offset);
        ensure!(
            client
                .read(&target, &target_object, offset as u64, length)
                .await?
                == data.slice(offset..offset + length),
            "resumed target body differs"
        );
        offset += length;
    }
    let upload = client.start_upload(&copied, &metadata, None).await?;
    let first = client
        .copy_part(&copied, &upload, 1, &source, &object, 0, WINDOW as u64)
        .await;
    let server_copy = !first
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<s3s::S3Error>())
        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::NotImplemented);
    let first = if server_copy {
        first?
    } else {
        client
            .upload_part(&copied, &upload, 1, data.slice(..WINDOW))
            .await?
    };
    let second = if server_copy {
        client
            .copy_part(
                &copied,
                &upload,
                2,
                &source,
                &object,
                WINDOW as u64,
                object.size - WINDOW as u64,
            )
            .await?
    } else {
        client
            .upload_part(&copied, &upload, 2, data.slice(WINDOW..))
            .await?
    };
    client
        .complete_upload(&copied, &upload, &[first, second], &Condition::Absent)
        .await?;
    let copy = client
        .head(&copied, None)
        .await?
        .context("server copy missing")?;
    ensure!(
        copy.size == object.size
            && client.read(&copied, &copy, WINDOW as u64, WINDOW).await?
                == data.slice(WINDOW..WINDOW * 2),
        "server copy differs"
    );
    let mut keys = Vec::new();
    let mut cursor = None;
    loop {
        let page = client.list(&prefix, cursor.as_deref(), 1).await?;
        keys.extend(page.objects.into_iter().map(|entry| entry.key));
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
        ensure!(keys.len() <= 3, "listing did not converge");
    }
    ensure!(
        keys.into_iter().collect::<BTreeSet<_>>()
            == BTreeSet::from([source.clone(), target.clone(), copied.clone()]),
        "listing renamed or lost an object"
    );
    let upload = client.start_upload(&abandoned, &metadata, None).await?;
    let (pending, more) = client.uploads(&abandoned).await?;
    ensure!(
        pending == [upload.clone()] && !more,
        "pending upload differs"
    );
    client.abort_upload(&abandoned, &upload).await?;
    client.abort_upload(&abandoned, &upload).await?;
    ensure!(
        client.parts(&abandoned, &upload, 0).await?.is_none(),
        "aborted upload remains"
    );
    let upload = client.start_upload(&target, &metadata, None).await?;
    let part = client
        .upload_part(
            &target,
            &upload,
            1,
            Bytes::from_static(b"must not overwrite"),
        )
        .await?;
    let completed = client
        .complete_upload(
            &target,
            &upload,
            &[part],
            &Condition::Match("\"wrong-target\"".into()),
        )
        .await;
    let conditional_complete = completed
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<s3s::S3Error>())
        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed);
    if completed.is_err() {
        ensure!(
            client.head(&target, None).await?.as_ref() == Some(&target_object),
            "rejected completion changed target"
        );
    }
    client.abort_upload(&target, &upload).await?;
    let deleted = client.delete(&source, &wrong).await;
    let conditional_delete = deleted
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<s3s::S3Error>())
        .is_some_and(|error| error.code() == &s3s::S3ErrorCode::PreconditionFailed);
    if deleted.is_err() {
        ensure!(
            client.head(&source, None).await?.as_ref() == Some(&object),
            "rejected deletion changed source"
        );
    }
    for key in [&source, &target, &copied] {
        if let Some(object) = client.head(key, None).await? {
            client.delete(key, &object).await?;
        }
    }
    println!(
        "external object transport: {}",
        serde_json::json!({"ranges":true,"multipart_resume":true,"server_part_copy":server_copy,"pagination":true,"conditional_complete":conditional_complete,"conditional_delete":conditional_delete,"version":object.version.is_some()})
    );
    Ok(())
}

#[test]
fn write_metadata_cannot_override_authentication_or_conditions() -> Result<()> {
    assert_eq!(
        result_tag(
            b"<CopyPartResult><ETag>copy</ETag></CopyPartResult>",
            "CopyPartResult",
            None
        )?,
        "\"copy\""
    );
    assert!(
        result_tag(
            b"<CopyPartResult><ETag>W/&quot;weak&quot;</ETag></CopyPartResult>",
            "CopyPartResult",
            None
        )
        .is_err()
    );
    let values = BTreeMap::from([
        ("content-type".into(), "image/png".into()),
        ("x-amz-meta-empty".into(), "".into()),
    ]);
    let headers = write_headers(&values, None)?;
    assert!(!headers.contains_key("x-amz-acl"));
    assert_eq!(
        write_headers(&values, Some(true))?["x-amz-acl"],
        "public-read"
    );
    for name in [
        "authorization",
        "host",
        "content-length",
        "if-match",
        "x-amz-acl",
        "x-amz-meta-",
    ] {
        assert!(write_headers(&BTreeMap::from([(name.into(), "x".into())]), None).is_err());
    }
    let mut headers = HeaderMap::new();
    assert!(
        Condition::Match("W/\"weak\"".into())
            .apply(&mut headers)
            .is_err()
    );
    for body in [
        b"<Error><Code>InvalidPart</Code></Error>".as_slice(),
        b"<CompleteMultipartUploadResult><Key>other</Key><ETag>one</ETag></CompleteMultipartUploadResult>",
        b"<CompleteMultipartUploadResult><Key>key</Key><ETag>one</ETag><ETag>two</ETag></CompleteMultipartUploadResult>",
    ] { assert!(result_tag(body, "CompleteMultipartUploadResult", Some("key")).is_err()); }
    for etag in ["*", "unquoted", "W/\"weak\"", "\"one\",\"two\""] {
        assert!(Condition::Match(etag.into()).apply(&mut headers).is_err());
    }
    headers.insert(header::ETAG, "\"one\"".parse()?);
    assert!(Written::headers(&headers, Some("\"two\"".into())).is_err());
    let error = result_tag(
        b"<Error><Code>PreconditionFailed</Code></Error>",
        "CompleteMultipartUploadResult",
        None,
    )
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<s3s::S3Error>().unwrap().code(),
        &s3s::S3ErrorCode::PreconditionFailed
    );
    assert!(
        response_error(b"<Error><Code>AccessDenied</Code><Code>InvalidPart</Code></Error>")
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn writes_preserve_conditions_receipts_and_versioned_permissions() -> Result<()> {
    use axum::{Router, routing::any};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let current = mode.clone();
    let count = calls.clone();
    let router = Router::new().fallback(any(move |request: Request<Body>| {
        let mode = current.load(Ordering::Relaxed);
        count.fetch_add(1, Ordering::Relaxed);
        async move {
            let (parts, body) = request.into_parts();
            let body = axum::body::to_bytes(body, 8192).await.unwrap();
            assert_eq!(hex::encode(Sha256::digest(&body)), parts.headers["x-amz-content-sha256"]);
            let query = parts.uri.query().unwrap_or("");
            let mut reply = Response::builder();
            let value = match (parts.method, query) {
                (Method::PUT, "") => {
                    assert_eq!(parts.headers[header::IF_NONE_MATCH], "*");
                    assert_eq!(parts.headers["x-amz-meta-empty"], "");
                    assert!(!parts.headers.contains_key("x-amz-acl"));
                    assert_eq!(body.as_ref(), b"abc");
                    reply = reply.header(header::ETAG, "\"put\"").header("x-amz-version-id", "v1");
                    ""
                }
                (Method::POST, "uploads=") => {
                    assert_eq!(parts.headers["x-amz-acl"], "public-read");
                    "<InitiateMultipartUploadResult><Key>imports/result</Key><UploadId>upload+/=</UploadId></InitiateMultipartUploadResult>"
                }
                (Method::PUT, "uploadId=upload%2B%2F%3D&partNumber=1") => {
                    if parts.headers.contains_key("x-amz-copy-source") {
                        assert_eq!(parts.headers["x-amz-copy-source"], "/photos/imports//a/..//%252F?versionId=v1%2B%2F%3D");
                        assert_eq!(parts.headers["x-amz-copy-source-if-match"], "\"source\"");
                        assert_eq!(parts.headers["x-amz-copy-source-range"], "bytes=0-2");
                        "<CopyPartResult><ETag>&quot;part&quot;</ETag></CopyPartResult>"
                    } else {
                        assert_eq!(body.as_ref(), b"abc");
                        reply = reply.header(header::ETAG, "\"part\"");
                        ""
                    }
                }
                (Method::GET, "uploadId=upload%2B%2F%3D&part-number-marker=0&max-parts=1000") =>
                    "<ListPartsResult><Key>imports/result</Key><UploadId>upload+/=</UploadId><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><ETag>&quot;part&quot;</ETag><Size>3</Size></Part></ListPartsResult>",
                (Method::GET, "uploads=&prefix=imports%2Fresult&max-uploads=128") =>
                    "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>imports/result</Key><UploadId>upload+/=</UploadId></Upload></ListMultipartUploadsResult>",
                (Method::POST, "uploadId=upload%2B%2F%3D") => {
                    assert_eq!(parts.headers[header::IF_MATCH], "\"put\"");
                    assert!(std::str::from_utf8(&body).unwrap().contains("<PartNumber>1</PartNumber>"));
                    reply = reply.header("x-amz-version-id", "v2");
                    if mode == 1 { "<Error><Code>InvalidPart</Code></Error>" }
                    else { "<CompleteMultipartUploadResult><Key>imports/result</Key><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>" }
                }
                (Method::PUT, "acl=&versionId=v1%2B%2F%3D") => {
                    assert_eq!(parts.headers["x-amz-acl"], "private");
                    if mode == 2 {
                        "<Error><Code>AccessControlListNotSupported</Code></Error>"
                    } else if mode == 3 {
                        reply = reply.status(403);
                        "<Error><Code>AccessDenied</Code><Message>untrusted detail</Message></Error>"
                    } else { "" }
                }
                (Method::GET, "acl=&versionId=v1%2B%2F%3D") =>
                    "<AccessControlPolicy><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
                (Method::DELETE, "uploadId=upload%2B%2F%3D") => { reply = reply.status(404); "" }
                (Method::DELETE, "versionId=v1%2B%2F%3D") => {
                    assert_eq!(parts.headers[header::IF_MATCH], "\"source\"");
                    reply = reply.status(204);
                    ""
                }
                value => panic!("unexpected external test request {value:?}"),
            };
            reply.body(Body::from(value)).unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let connection = Connection {
        endpoint_url: format!("http://{address}"),
        region: "test".into(),
        bucket: "photos".into(),
        prefix: "imports".into(),
        virtual_hosted: false,
        allow_http: true,
    };
    let options = Options {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        retry_timeout: Duration::from_secs(1),
        max_retries: 0,
    };
    let client = connection.connect("test-access", "test-secret", options)?;
    let metadata = BTreeMap::from([("x-amz-meta-empty".into(), "".into())]);
    let written = client
        .put(
            "result",
            &Condition::Absent,
            &metadata,
            None,
            Bytes::from_static(b"abc"),
        )
        .await?;
    assert_eq!(written.version.as_deref(), Some("v1"));
    let upload = client.start_upload("result", &metadata, Some(true)).await?;
    assert_eq!(
        client.uploads("result").await?,
        (vec![upload.clone()], false)
    );
    let part = client
        .upload_part("result", &upload, 1, Bytes::from_static(b"abc"))
        .await?;
    let listed = client.parts("result", &upload, 0).await?.unwrap();
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].etag, part.etag);
    assert!(listed.next.is_none());
    let mut object = Object {
        size: 3,
        etag: "\"source\"".into(),
        version: Some("v1+/=".into()),
        modified: None,
        metadata: BTreeMap::new(),
    };
    assert_eq!(
        client
            .copy_part("result", &upload, 1, "/a/..//%2F", &object, 0, 3)
            .await?
            .etag,
        part.etag
    );
    let condition = Condition::Match(written.etag);
    let written = client
        .complete_upload("result", &upload, &listed.items, &condition)
        .await?;
    assert_eq!(
        written,
        Written {
            etag: "\"done\"".into(),
            version: Some("v2".into())
        }
    );
    mode.store(1, Ordering::Relaxed);
    assert!(
        client
            .complete_upload("result", &upload, &listed.items, &condition)
            .await
            .is_err()
    );
    client.set_acl("result", &object, false).await?;
    for (value, code) in [
        (2, s3s::S3ErrorCode::AccessControlListNotSupported),
        (3, s3s::S3ErrorCode::AccessDenied),
    ] {
        mode.store(value, Ordering::Relaxed);
        let error = client.set_acl("result", &object, false).await.unwrap_err();
        assert_eq!(error.downcast_ref::<s3s::S3Error>().unwrap().code(), &code);
        assert!(!error.to_string().contains("untrusted detail"));
    }
    mode.store(0, Ordering::Relaxed);
    assert!(!client.acl("result", &object).await?);
    client.delete("result", &object).await?;
    client.abort_upload("result", &upload).await?;
    object.version = None;
    let before = calls.load(Ordering::Relaxed);
    assert!(client.set_acl("result", &object, true).await.is_err());
    assert!(
        client
            .complete_upload("result", &upload, &[], &condition)
            .await
            .is_err()
    );
    assert!(
        client
            .upload_part("result", &upload, 0, Bytes::new())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::Relaxed), before);
    server.abort();
    Ok(())
}

#[test]
fn part_windows_cover_the_object_with_bounded_receipts() -> Result<()> {
    for size in [
        0,
        1,
        WINDOW as u64,
        10000 * WINDOW as u64,
        10000 * WINDOW as u64 + 1,
        MAX_SIZE,
    ] {
        let part = part_size(size)?;
        assert!(part >= 5 * 1024 * 1024 && part <= MAX_PART);
        assert_eq!(part % WINDOW as u64, 0);
        assert!(size.div_ceil(part) <= 10000);
    }
    assert!(part_size(MAX_SIZE + 1).is_err());
    Ok(())
}

#[tokio::test]
async fn response_slices_retain_their_work_memory() -> Result<()> {
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let mut client = Connection {
        endpoint_url: "https://example.invalid".into(),
        region: "test".into(),
        bucket: "bucket".into(),
        prefix: String::new(),
        virtual_hosted: false,
        allow_http: false,
    }
    .connect(
        "test-access",
        "test-secret",
        Options {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            retry_timeout: Duration::from_secs(1),
            max_retries: 0,
        },
    )?;
    client.memory = Some(std::sync::Arc::new(gate.clone().acquire_owned().await?));
    let bytes = client.bounded(Body::from("verified"), 8).await?;
    let tail = bytes.slice(2..);
    drop(bytes);
    drop(client);
    assert_eq!(gate.available_permits(), 0);
    assert_eq!(tail, "rified");
    drop(tail);
    assert_eq!(gate.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn streamed_parts_check_replays_and_idle_timeouts() -> Result<()> {
    use axum::{Router, routing::any};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let mode = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicBool::new(false));
    let state = (mode.clone(), received.clone());
    let router = Router::new().fallback(any(move |request: Request<Body>| {
        let (mode, received) = state.clone();
        async move {
            let mode = mode.load(Ordering::Relaxed);
            if mode == 1 {
                return Response::builder()
                    .header("etag", "\"part\"")
                    .body(Body::empty())
                    .unwrap();
            }
            let (headers, body) = request.into_parts();
            let mut body = body.into_data_stream();
            let mut digest = Sha256::new();
            let mut size = 0u64;
            while let Some(bytes) = body.next().await {
                let Ok(bytes) = bytes else {
                    return Response::builder().status(400).body(Body::empty()).unwrap();
                };
                received.store(true, Ordering::Relaxed);
                size += bytes.len() as u64;
                digest.update(&bytes);
            }
            if mode == 0 {
                assert_eq!(size.to_string(), headers.headers[header::CONTENT_LENGTH]);
                assert_eq!(
                    hex::encode(digest.finalize()),
                    headers.headers["x-amz-content-sha256"]
                );
            }
            Response::builder()
                .header("etag", "\"part\"")
                .body(Body::empty())
                .unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = Connection {
        endpoint_url: format!("http://{address}"),
        region: "test".into(),
        bucket: "bucket".into(),
        prefix: String::new(),
        virtual_hosted: false,
        allow_http: true,
    }
    .connect(
        "test-access",
        "test-secret",
        Options {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(500),
            retry_timeout: Duration::from_secs(1),
            max_retries: 0,
        },
    )?;
    let block = Bytes::from(vec![7; 32 * 1024]);
    let mut digest = Sha256::new();
    for _ in 0..6 {
        digest.update(&block);
    }
    let digest: [u8; 32] = digest.finalize().into();
    let size = block.len() as u64 * 6;
    let seen = received.clone();
    let source = async_stream::try_stream! {
        for index in 0..6 {
            yield block.clone();
            tokio::time::sleep(Duration::from_millis(150)).await;
            if index == 2 { assert!(seen.load(Ordering::Relaxed), "upload buffered the entire source"); }
        }
    };
    let part = client
        .upload_part_stream("object", "upload", 1, size, &digest, Box::pin(source))
        .await?;
    assert_eq!(part.size, size);
    mode.store(2, Ordering::Relaxed);
    let data = Bytes::from_static(b"1234");
    let good: [u8; 32] = Sha256::digest(&data).into();
    for (length, digest) in [(5, good), (3, good), (4, [0; 32])] {
        let data = data.clone();
        let source = Box::pin(futures_util::stream::once(async move { Ok(data) }));
        assert!(
            client
                .upload_part_stream("object", "upload", 1, length, &digest, source)
                .await
                .is_err()
        );
    }
    for mode_value in [0, 1] {
        mode.store(mode_value, Ordering::Relaxed);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            client.upload_part_stream(
                "object",
                "upload",
                1,
                4,
                &good,
                Box::pin(futures_util::stream::pending()),
            ),
        )
        .await?;
        assert!(result.is_err(), "accepted a stalled or incomplete source");
    }
    server.abort();
    Ok(())
}

#[test]
fn listings_preserve_keys_and_require_complete_pagination() -> Result<()> {
    let body = b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next+/=</NextContinuationToken><EncodingType>url</EncodingType><Contents><Key>imports%2Fa%2F..%2F%2Fb%252F</Key><Size>23</Size></Contents><Contents><Key>imports%2Fa%20%2B+%2520</Key><Size>0</Size></Contents></ListBucketResult>";
    let page = listing(body, "imports/", "imports/a", None, 2)?;
    assert_eq!(
        page.objects
            .iter()
            .map(|entry| (entry.key.as_str(), entry.size))
            .collect::<Vec<_>>(),
        [("a/..//b%2F", 23), ("a + %20", 0)]
    );
    assert_eq!(page.next.as_deref(), Some("next+/="));
    assert!(listing(body, "imports/", "imports/a", page.next.as_deref(), 2).is_err());
    assert!(listing(body, "imports/", "imports/a", None, 1).is_err());
    assert!(listing(body, "imports/", "imports/other", None, 2).is_err());
    let text = std::str::from_utf8(body)?;
    for value in [
        text.replace("<IsTruncated>true</IsTruncated>", ""),
        text.replace(
            "<IsTruncated>true</IsTruncated>",
            "<IsTruncated>false</IsTruncated>",
        ),
        text.replace("<NextContinuationToken>next+/=</NextContinuationToken>", ""),
        text.replace("<Size>23</Size>", "<Size>-1</Size>"),
        text.replace("<Size>23</Size>", "<Size>23</Size><Size>23</Size>"),
        text.replace("imports%2Fa%20%2B+%2520", "imports%2Fa%2F..%2F%2Fb%252F"),
        format!("{text}<extra/>"),
    ] {
        assert!(
            listing(value.as_bytes(), "imports/", "imports/a", None, 2).is_err(),
            "accepted {value}"
        );
    }
    assert!(
        listing(
            b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            "imports/",
            "imports/",
            None,
            2
        )?
        .objects
        .is_empty()
    );
    Ok(())
}

#[test]
fn snapshots_reject_ambiguous_headers_and_keep_empty_metadata() -> Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_LENGTH, "10".parse()?);
    headers.insert(header::ETAG, "\"first\"".parse()?);
    headers.insert("x-amz-version-id", "null".parse()?);
    headers.insert("x-amz-meta-empty", "".parse()?);
    let object = Object::headers(&headers)?;
    assert_eq!(object.version, None);
    assert_eq!(
        object.metadata.get("x-amz-meta-empty").map(String::as_str),
        Some("")
    );
    headers.append(header::ETAG, "\"first\"".parse()?);
    assert!(Object::headers(&headers).is_err());
    headers.insert(header::ETAG, "W/\"first\"".parse()?);
    assert!(Object::headers(&headers).is_err());
    headers.insert(header::ETAG, "\"first\"".parse()?);
    headers.append("x-amz-meta-empty", "other".parse()?);
    assert!(Object::headers(&headers).is_err());
    Ok(())
}

#[tokio::test]
async fn versioned_ranges_are_bounded_and_reject_changed_sources() -> Result<()> {
    use axum::{Router, routing::any};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let mode = Arc::new(AtomicUsize::new(0));
    let count = Arc::new(AtomicUsize::new(0));
    let current = mode.clone();
    let calls = count.clone();
    let router = Router::new().fallback(any(move |request: Request<Body>| {
        let mode = current.load(Ordering::Relaxed);
        calls.fetch_add(1, Ordering::Relaxed);
        async move {
            assert_eq!(request.uri().path(), "/photos/imports//a/..//%252F");
            assert!(request.headers().contains_key(header::AUTHORIZATION));
            let head = request.method() == Method::HEAD;
            if !head {
                assert_eq!(request.uri().query(), Some("versionId=v1%2B%2F%3D"));
                assert_eq!(request.headers()[header::IF_MATCH], "\"first\"");
                assert_eq!(request.headers()[header::RANGE], "bytes=3-7");
            }
            let mut reply = Response::builder()
                .status(if head || mode == 1 { 200 } else { 206 })
                .header(header::CONTENT_LENGTH, if head { "10" } else { "5" })
                .header(
                    header::ETAG,
                    if !head && mode == 2 {
                        "\"second\""
                    } else {
                        "\"first\""
                    },
                )
                .header(
                    "x-amz-version-id",
                    if !head && mode == 3 { "v2" } else { "v1+/=" },
                )
                .header(
                    header::LAST_MODIFIED,
                    if !head && mode == 4 {
                        "Fri, 30 Oct 2026 08:00:01 GMT"
                    } else {
                        "Fri, 30 Oct 2026 08:00:00 GMT"
                    },
                )
                .header(header::CONTENT_TYPE, "image/png")
                .header(
                    "x-amz-meta-empty",
                    if !head && mode == 5 { "changed" } else { "" },
                );
            if !head {
                reply = reply.header(
                    header::CONTENT_RANGE,
                    if mode == 6 {
                        "bytes 3-7/11"
                    } else {
                        "bytes 3-7/10"
                    },
                );
            }
            reply
                .body(if head {
                    Body::empty()
                } else {
                    Body::from("34567")
                })
                .unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let connection = Connection {
        endpoint_url: format!("http://{address}"),
        region: "test".into(),
        bucket: "photos".into(),
        prefix: "imports".into(),
        virtual_hosted: false,
        allow_http: true,
    };
    let options = Options {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        retry_timeout: Duration::from_secs(1),
        max_retries: 0,
    };
    let client = connection.connect("test-access", "test-secret", options)?;
    let object = client.head("/a/..//%2F", None).await?.unwrap();
    assert_eq!(client.read("/a/..//%2F", &object, 3, 5).await?, "34567");
    for value in 1..=6 {
        mode.store(value, Ordering::Relaxed);
        assert!(
            client.read("/a/..//%2F", &object, 3, 5).await.is_err(),
            "accepted changed source {value}"
        );
    }
    let before = count.load(Ordering::Relaxed);
    assert!(
        client
            .read("/a/..//%2F", &object, u64::MAX, 2)
            .await
            .is_err()
    );
    assert!(
        client
            .read("/a/..//%2F", &object, 0, WINDOW + 1)
            .await
            .is_err()
    );
    assert!(client.read("/a/..//%2F", &object, 0, 0).await.is_err());
    assert_eq!(count.load(Ordering::Relaxed), before);
    server.abort();
    Ok(())
}
