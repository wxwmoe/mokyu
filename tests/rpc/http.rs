use super::*;

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and real signed HTTP requests"]
async fn node_http_reads_use_one_rpc_and_verify_signatures() -> Result<()> {
    node_http_reads(false, false).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and real cached HTTP reads"]
async fn node_http_cache_validates_versions_and_recovers() -> Result<()> {
    node_http_reads(true, false).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and disposable node cache files"]
async fn node_http_file_cache_recovers_and_falls_back() -> Result<()> {
    node_http_reads(false, true).await
}

async fn node_http_reads(cache: bool, disk: bool) -> Result<()> {
    use crate::{
        config::node::NodeConfig,
        node::{NodeContext, Phase},
    };
    use hyper_util::rt::TokioIo;
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let directory = path.parent().unwrap().join("http-runtime");
    tokio::fs::create_dir(&directory).await?;
    crate::cluster::identity::write_state(&directory.join("node.json"), &identity).await?;
    let config = format!(
        "[instance]\ncontrol_url='{url}'\ndata_path='{}'\nresponse_cache_size='{}'\ncache_size='16MiB'\ncache_entries=128\n",
        directory.display(),
        if cache { "16MiB" } else { "0B" }
    );
    let mut context = NodeContext::new(NodeConfig::parse(&config, "data")?, "data").await?;
    let dns = Dns::new().await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 2)]);
    Arc::get_mut(&mut context).unwrap().client = dns.client(&url, identity)?;
    let mut states = context.subscribe();
    let driver = tokio::spawn(context.clone().run());
    node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    let reads = Arc::new(crate::node::reads::Reads::new(context.clone())?);
    let gateway = crate::http::node::Gateway::new(reads.clone())?;
    let service = gateway.service()?;
    let s3_listener = tokio::net::TcpListener::bind("127.0.0.1:19100").await?;
    let web_listener = tokio::net::TcpListener::bind("127.0.0.1:19101").await?;
    let versions = Arc::new([
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
    ]);
    let endpoints = serde_json::json!({"s3": s3_listener.local_addr()?.port(), "web": web_listener.local_addr()?.port()});
    tokio::fs::write(
        path.parent().unwrap().join("http-ports.json"),
        serde_json::to_vec(&endpoints)?,
    )
    .await?;
    let s3_gateway = gateway.clone();
    let s3_versions = versions.clone();
    let s3_server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (socket, _) = s3_listener.accept().await?;
            socket.set_nodelay(true)?;
            let gateway = s3_gateway.clone();
            let service = service.clone();
            let versions = s3_versions.clone();
            connections.spawn(async move {
                let handler = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| {
                        versions[usize::from(request.version() == hyper::Version::HTTP_2)]
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let (gateway, service) = (gateway.clone(), service.clone());
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                gateway.s3(&service, request.map(s3s::Body::from)).await,
                            )
                        }
                    },
                );
                let _ = crate::http::transport::builder(64)
                    .serve_connection(TokioIo::new(socket), handler)
                    .await;
            });
            while connections.try_join_next().is_some() {}
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    });
    let web_versions = versions.clone();
    let web_server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (socket, _) = web_listener.accept().await?;
            socket.set_nodelay(true)?;
            let gateway = gateway.clone();
            let versions = web_versions.clone();
            connections.spawn(async move {
                let handler = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| {
                        versions[usize::from(request.version() == hyper::Version::HTTP_2)]
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let gateway = gateway.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                gateway.public(request.map(axum::body::Body::new)).await,
                            )
                        }
                    },
                );
                let _ = crate::http::transport::builder(64)
                    .serve_connection(TokioIo::new(socket), handler)
                    .await;
            });
            while connections.try_join_next().is_some() {}
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    });
    if disk {
        file_cache_reads(&reads).await?;
    } else if cache {
        cache_reads(&reads).await?;
    } else {
        fixture("http-signed").await?;
        fixture("http-sources").await?;
        fixture("http-public").await?;
        let before: Vec<_> = versions
            .iter()
            .map(|count| count.load(std::sync::atomic::Ordering::Relaxed))
            .collect();
        fixture("http-proxy").await?;
        assert_eq!(
            versions[0].load(std::sync::atomic::Ordering::Relaxed),
            before[0],
            "proxy used an HTTP/1 upstream"
        );
        assert!(
            versions[1].load(std::sync::atomic::Ordering::Relaxed) >= before[1] + 8,
            "proxy did not exercise h2c upstreams"
        );
        let mut compression = Vec::new();
        while let Some(memory) = crate::node::delivery::compression_work(&reads) {
            compression.push(memory);
        }
        assert!(!compression.is_empty());
        fixture("http-compression-busy").await?;
        drop(compression);
        let before = reads.backend_gets();
        fixture("http-rejected").await?;
        assert_eq!(
            reads.backend_gets(),
            before,
            "rejected signatures or metadata-only responses read object bytes"
        );
        fixture("http-pending").await?;
        fixture("assert-no-reads").await?;
    }
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    if disk {
        let before = reads.backend_gets();
        fixture("l2-reconnected").await?;
        assert_eq!(reads.backend_gets(), before);
    } else if cache {
        let before = reads.backend_gets();
        fixture("l1-reconnected").await?;
        assert_eq!(
            reads.backend_gets(),
            before,
            "control recovery lost the reusable body"
        );
    } else {
        fixture("http-reconnected").await?;
    }
    fixture("assert-no-reads").await?;
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    let point = if disk {
        "node-cache-decode"
    } else if cache {
        "response-cache-body"
    } else {
        "http-compression-finish"
    };
    tokio::fs::remove_file(faults.join(format!("{point}.hit")))
        .await
        .ok();
    tokio::fs::write(faults.join(point), b"pause").await?;
    let fenced = tokio::spawn(fixture(if disk {
        "l2-fenced"
    } else if cache {
        "l1-fenced"
    } else {
        "http-fenced"
    }));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !faults.join(format!("{point}.hit")).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    context.stop();
    fenced.await??;
    tokio::fs::remove_file(faults.join(point)).await?;
    driver.await??;
    s3_server.abort();
    web_server.abort();
    let _ = s3_server.await;
    let _ = web_server.await;
    Ok(())
}

async fn file_cache_reads(reads: &Arc<crate::node::reads::Reads>) -> Result<()> {
    async fn settled(reads: &crate::node::reads::Reads) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while reads.file_cache().await["fills"].as_u64().unwrap() != 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }
    fixture("l2-warm").await?;
    settled(reads).await?;
    assert!(reads.file_cache().await["entries"].as_u64().unwrap() > 0);
    let before = reads.backend_gets();
    fixture("l2-hot").await?;
    assert_eq!(reads.backend_gets(), before);
    fixture("l2-corrupt").await?;
    assert!(
        reads.backend_gets() > before,
        "bad disk cache did not fall back to storage"
    );
    settled(reads).await?;
    let before = reads.backend_gets();
    fixture("l2-repaired").await?;
    assert_eq!(reads.backend_gets(), before);
    fixture("l2-pack-warm").await?;
    settled(reads).await?;
    let before = reads.backend_gets();
    fixture("l2-pack-hot").await?;
    assert_eq!(
        reads.backend_gets(),
        before,
        "cached pack members downloaded the pack again"
    );
    fixture("l2-disabled").await?;
    assert!(reads.backend_gets() > before);
    let before = reads.backend_gets();
    fixture("l2-enabled").await?;
    assert_eq!(reads.backend_gets(), before);
    settled(reads).await?;

    let initial = reads.file_cache().await;
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::write(faults.join("node-raw-before-verify"), b"pause").await?;
    let cancelled = tokio::spawn(fixture("l2-raw-cancel"));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !faults.join("node-raw-before-verify.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert!(
        reads.file_cache().await["bytes"].as_u64().unwrap() > initial["bytes"].as_u64().unwrap()
    );
    cancelled.await??;
    tokio::fs::remove_file(faults.join("node-raw-before-verify")).await?;
    settled(reads).await?;
    assert_eq!(
        reads.file_cache().await["bytes"],
        initial["bytes"],
        "cancelled raw fill retained disk capacity"
    );
    assert_eq!(reads.file_cache().await["entries"], initial["entries"]);
    let before = reads.backend_gets();
    fixture("l2-raw-warm").await?;
    assert_eq!(
        reads.backend_gets() - before,
        2,
        "concurrent raw miss downloaded more than one HEAD and body"
    );
    let before = reads.backend_gets();
    fixture("l2-raw-hot").await?;
    assert_eq!(reads.backend_gets(), before);
    fixture("l2-raw-corrupt").await?;
    assert_eq!(
        reads.backend_gets() - before,
        2,
        "bad raw cache did not recover in one download"
    );
    let before = reads.backend_gets();
    fixture("l2-raw-repaired").await?;
    assert_eq!(reads.backend_gets(), before);
    fixture("l2-raw-disabled").await?;
    assert!(reads.backend_gets() > before);
    let before = reads.backend_gets();
    fixture("l2-raw-enabled").await?;
    assert_eq!(reads.backend_gets(), before);
    settled(reads).await?;
    let reopened = Arc::new(crate::node::reads::Reads::new(reads.node.clone())?);
    let raw = tokio::fs::read(
        PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?)
            .parent()
            .unwrap()
            .join("raw.bin"),
    )
    .await?;
    for (bucket, key, expected) in [
        (
            "read-objects",
            "l1-hot",
            b"node cache source\n".repeat(4096),
        ),
        ("read-raw", "raw", raw),
    ] {
        let prepared = reopened
            .open(wire::ReadRequest {
                route: Some(wire::ReadRoute {
                    route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                        name: bucket.into(),
                        key: key.into(),
                        ..Default::default()
                    })),
                }),
                method: wire::ReadMethod::Get as i32,
                surface: wire::ReadSurface::S3 as i32,
                include: vec![
                    wire::ReadInclude::Object as i32,
                    wire::ReadInclude::Mapping as i32,
                ],
                ..Default::default()
            })
            .await?;
        let mut stream = reopened.body(prepared);
        let mut body = Vec::new();
        while let Some(bytes) = stream.next().await {
            body.extend_from_slice(&bytes?);
        }
        assert_eq!(body, expected);
    }
    assert_eq!(
        reopened.backend_gets(),
        0,
        "reopened cache lost its physical identities"
    );
    assert!(reads.file_cache().await["bytes"].as_u64().unwrap() <= 16 * 1024 * 1024);
    fixture("assert-no-reads").await?;
    Ok(())
}

async fn cache_reads(reads: &Arc<crate::node::reads::Reads>) -> Result<()> {
    fixture("l1-warm").await?;
    assert!(reads.body_cache()["entries"].as_u64().unwrap() > 0);
    let before = reads.backend_gets();
    fixture("l1-hot").await?;
    fixture("l1-derived").await?;
    let mut encoders = Vec::new();
    while let Some(memory) = crate::node::delivery::compression_work(reads) {
        encoders.push(memory);
    }
    assert!(!encoders.is_empty());
    fixture("l1-encoded-busy").await?;
    drop(encoders);
    fixture("l1-denied").await?;
    assert_eq!(
        reads.backend_gets(),
        before,
        "cached or rejected reads reached storage"
    );
    assert!(reads.body_cache()["hits"].as_u64().unwrap() >= 12);
    fixture("l1-cancel-first").await?;
    let entries = reads.body_cache()["entries"].as_u64().unwrap();
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::write(faults.join("http-compression-finish"), b"pause").await?;
    let cancelled = tokio::spawn(fixture("l1-cancelled"));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !faults.join("http-compression-finish.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    cancelled.await??;
    tokio::fs::remove_file(faults.join("http-compression-finish")).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while reads.body_cache()["building"].as_u64().unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(
        reads.body_cache()["entries"].as_u64().unwrap(),
        entries,
        "cancelled encoder published a cache entry"
    );
    fixture("l1-off").await?;
    assert!(
        reads.backend_gets() > before,
        "changed policy reused a stale cache proof"
    );
    let before = reads.backend_gets();
    fixture("l1-off-hot").await?;
    assert_eq!(
        reads.backend_gets(),
        before,
        "unchanged off policy bypassed the cache"
    );
    fixture("l1-inline").await?;
    assert!(
        reads.backend_gets() > before,
        "inline accepted unchecked cached bytes"
    );
    let before = reads.backend_gets();
    fixture("l1-replace").await?;
    assert!(
        reads.backend_gets() > before,
        "object replacement reused stale bytes"
    );
    fixture("l1-warm-replaced").await?;
    let before = reads.backend_gets();
    fixture("l1-hot-replaced").await?;
    assert_eq!(reads.backend_gets(), before);
    let cache = reads.body_cache();
    assert!(
        cache["live_bytes"].as_u64().unwrap() + cache["metadata_bytes"].as_u64().unwrap()
            <= cache["limit_bytes"].as_u64().unwrap()
    );
    fixture("assert-no-reads").await?;
    Ok(())
}
