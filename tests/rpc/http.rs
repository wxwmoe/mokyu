use super::*;

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and real signed HTTP requests"]
async fn node_http_reads_use_one_rpc_and_verify_signatures() -> Result<()> {
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
        "[instance]\ncontrol_url='{url}'\ndata_path='{}'\n",
        directory.display()
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
    fixture("http-reconnected").await?;
    fixture("assert-no-reads").await?;
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::remove_file(faults.join("http-compression-finish.hit"))
        .await
        .ok();
    tokio::fs::write(faults.join("http-compression-finish"), b"pause").await?;
    let fenced = tokio::spawn(fixture("http-fenced"));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !faults.join("http-compression-finish.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    context.stop();
    fenced.await??;
    tokio::fs::remove_file(faults.join("http-compression-finish")).await?;
    driver.await??;
    s3_server.abort();
    web_server.abort();
    let _ = s3_server.await;
    let _ = web_server.await;
    Ok(())
}
