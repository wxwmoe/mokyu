use super::*;

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and real signed HTTP requests"]
async fn node_http_reads_use_one_rpc_and_verify_signatures() -> Result<()> {
    node_http_reads(HttpCase::Reads).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and real cached HTTP reads"]
async fn node_http_cache_validates_versions_and_recovers() -> Result<()> {
    node_http_reads(HttpCase::Memory).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and disposable node cache files"]
async fn node_http_file_cache_recovers_and_falls_back() -> Result<()> {
    node_http_reads(HttpCase::Files).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and public cache mutations"]
async fn node_http_public_cache_expires_and_recovers() -> Result<()> {
    node_http_reads(HttpCase::Public).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and observation queries"]
async fn node_http_observations_are_batched_and_recover() -> Result<()> {
    node_http_reads(HttpCase::Observations).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and unique local and staged sources"]
async fn node_http_unique_sources_verify_and_recover() -> Result<()> {
    node_http_reads(HttpCase::Sources).await
}

enum HttpCase {
    Copy,
    Objects,
    Catalog,
    Raw,
    Writes,
    Reads,
    Memory,
    Files,
    Public,
    Observations,
    Sources,
}

#[tokio::test]
#[ignore = "requires isolated controls, object storage and signed copy requests"]
async fn node_http_copy_preserves_sources_and_recovers_batches() -> Result<()> {
    node_http_reads(HttpCase::Copy).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and signed object mutations"]
async fn node_http_objects_batch_authorization_and_recover_lost_results() -> Result<()> {
    node_http_reads(HttpCase::Objects).await
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and signed catalog requests"]
async fn node_http_catalog_uses_one_rpc_and_recovers() -> Result<()> {
    node_http_reads(HttpCase::Catalog).await
}

#[tokio::test]
#[ignore = "requires isolated controls, encrypted object storage and signed HTTP writes"]
async fn node_http_writes_stream_batches_and_publish_after_validation() -> Result<()> {
    node_http_reads(HttpCase::Writes).await
}

#[tokio::test]
#[ignore = "requires isolated controls, raw object storage and signed HTTP writes"]
async fn node_http_raw_writes_deduplicate_and_recover() -> Result<()> {
    node_http_reads(HttpCase::Raw).await
}

async fn unique_source_reads(reads: &Arc<crate::node::reads::Reads>) -> Result<()> {
    fixture("sources-setup").await?;
    let before = reads.backend_gets();
    for action in [
        "sources-local",
        "sources-compressed",
        "sources-local-corrupt",
        "sources-foreign",
        "sources-restored",
    ] {
        fixture(action).await?;
    }
    assert_eq!(
        reads.backend_gets(),
        before,
        "local sources used backend I/O"
    );
    fixture("sources-staged").await?;
    assert_eq!(
        reads.backend_gets() - before,
        12,
        "staged reads fetched unexpected bodies"
    );
    fixture("sources-staged-corrupt").await?;
    fixture("assert-no-reads").await?;
    assert_eq!(reads.body_cache()["entries"], 0);
    assert_eq!(reads.file_cache().await["entries"], 0);
    Ok(())
}

async fn observation_reads(reads: &Arc<crate::node::reads::Reads>) -> Result<()> {
    let reports = &reads.node.observations;
    fixture("observations-invalid").await?;
    assert_eq!(reports.activity.snapshot()["completed"], 0);
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::write(faults.join("node-observations-before-batch"), b"pause").await?;
    fixture("observations-batch").await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !faults.join("node-observations-before-batch.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(reports.compression.snapshot()["pending"], 3);
    assert_eq!(reports.activity.snapshot()["pending"], 1);
    fixture("observations-before").await?;
    tokio::fs::remove_file(faults.join("node-observations-before-batch")).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while reports.compression.snapshot()["written"].as_u64().unwrap() != 3
            || reports.activity.snapshot()["completed"].as_u64().unwrap() != 1
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    fixture("observations-written").await?;
    fixture("observations-repeat").await?;
    let written = reports.compression.snapshot()["written"].as_u64().unwrap();
    tokio::fs::write(faults.join("http-compression-finish"), b"pause").await?;
    let cancelled = tokio::spawn(fixture("observations-cancel"));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !faults.join("http-compression-finish.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    cancelled.await??;
    tokio::fs::remove_file(faults.join("http-compression-finish")).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(reports.compression.snapshot()["written"], written);
    fixture("observations-cancelled").await?;
    let failed = reports.compression.snapshot()["failed"].as_u64().unwrap();
    tokio::fs::write(faults.join("node-observations-before-report"), b"pause").await?;
    fixture("observations-timeout").await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while reports.compression.snapshot()["failed"].as_u64().unwrap() == failed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    tokio::fs::remove_file(faults.join("node-observations-before-report")).await?;
    fixture("observations-failed").await?;
    fixture("observations-retry").await?;
    let runtime = reads.node.statistics.runtime();
    assert!(runtime["http"]["s3"]["completed"].as_u64().unwrap() > 0);
    assert!(runtime["http"]["web"]["completed"].as_u64().unwrap() > 0);
    assert!(runtime["http"]["web"]["bytes"].as_u64().unwrap() > 0);
    Ok(())
}

async fn public_cache_reads(reads: &Arc<crate::node::reads::Reads>) -> Result<()> {
    async fn populated(reads: &crate::node::reads::Reads, body: bool) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while reads.public_cache()["entries"].as_u64().unwrap() == 0
                || (body && reads.body_cache()["entries"].as_u64().unwrap() == 0)
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }
    let before = reads.backend_gets();
    fixture("public-head-warm").await?;
    populated(reads, false).await?;
    fixture("public-head-hot").await?;
    assert_eq!(
        reads.backend_gets(),
        before,
        "cached HEAD read object bytes"
    );
    fixture("public-body-cold").await?;
    populated(reads, true).await?;
    assert!(
        reads.backend_gets() > before,
        "HEAD snapshot authorized a missing body"
    );
    let before = reads.backend_gets();
    fixture("public-body-hot").await?;
    fixture("public-strong").await?;
    assert_eq!(reads.backend_gets(), before);
    fixture("public-strict").await?;
    assert!(
        reads.backend_gets() > before,
        "changed storage policy reused an obsolete body proof"
    );
    fixture("public-refresh-warm").await?;
    populated(reads, true).await?;
    let refreshed = reads.public_cache()["refreshed"].as_u64().unwrap();
    fixture("public-refresh-trigger").await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while reads.public_cache()["refreshed"].as_u64().unwrap() == refreshed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    fixture("public-refreshed").await?;
    fixture("public-expiry-warm").await?;
    populated(reads, true).await?;
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::write(faults.join("node-public-refresh"), b"pause").await?;
    fixture("public-refresh-stale").await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !faults.join("node-public-refresh.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    fixture("public-expired").await?;
    tokio::fs::remove_file(faults.join("node-public-refresh")).await?;
    fixture("public-restore").await?;
    populated(reads, true).await?;
    fixture("public-clear").await?;
    tokio::fs::write(faults.join("node-public-fill"), b"pause").await?;
    fixture("public-late-start").await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !faults.join("node-public-fill.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    fixture("public-late-revoke").await?;
    tokio::fs::remove_file(faults.join("node-public-fill")).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture("public-late-denied").await?;
    fixture("public-restore").await?;
    populated(reads, true).await?;
    let state = reads.public_cache();
    assert!(state["used_bytes"].as_u64().unwrap() <= state["limit_bytes"].as_u64().unwrap());
    assert!(state["hits"].as_u64().unwrap() > 0);
    Ok(())
}

async fn node_http_reads(case: HttpCase) -> Result<()> {
    let copy = matches!(case, HttpCase::Copy);
    let objects = matches!(case, HttpCase::Objects);
    let catalog = matches!(case, HttpCase::Catalog);
    let writes = matches!(case, HttpCase::Writes);
    let raw = matches!(case, HttpCase::Raw);
    let cache = matches!(case, HttpCase::Memory | HttpCase::Public | HttpCase::Writes);
    let disk = matches!(case, HttpCase::Files);
    let public = matches!(case, HttpCase::Public);
    let observations = matches!(case, HttpCase::Observations);
    let sources = matches!(case, HttpCase::Sources);
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
    let observed = gateway.clone();
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
    if copy {
        let before = reads.backend_gets();
        fixture("http-copy-references").await?;
        assert_eq!(
            reads.backend_gets(),
            before,
            "reference copy read backend bytes"
        );
        fixture("http-copy").await?;
    } else if objects {
        let before = observed.write_resources();
        fixture("http-objects").await?;
        assert_eq!(observed.write_resources(), before);
        assert_eq!(reads.backend_gets(), 0);
    } else if catalog {
        let before = observed.write_resources();
        let controls = observed.control_slots();
        let available = controls.available_permits();
        fixture("http-catalog").await?;
        assert_eq!(controls.available_permits(), available);
        let mut held = Vec::new();
        while let Ok(permit) = controls.clone().try_acquire_owned() {
            held.push(permit);
        }
        fixture("http-catalog-busy").await?;
        drop(held);
        assert_eq!(observed.write_resources(), before);
        assert_eq!(reads.backend_gets(), 0);
    } else if raw {
        fixture("http-raw").await?;
    } else if writes {
        fixture("http-writes").await?;
    } else if sources {
        unique_source_reads(&reads).await?;
    } else if observations {
        observation_reads(&reads).await?;
    } else if public {
        public_cache_reads(&reads).await?;
    } else if disk {
        file_cache_reads(&reads).await?;
    } else if cache {
        cache_reads(&reads).await?;
    } else {
        fixture("http-signed").await?;
        fixture("http-sources").await?;
        fixture("http-public").await?;
        let cors = observed.cors_cache();
        assert!(cors["hits"].as_u64().unwrap() > 0);
        assert_eq!(cors["entries"], 1);
        fixture("http-cors-disabled").await?;
        assert_eq!(observed.cors_cache()["hits"], cors["hits"]);
        fixture("http-cors-update").await?;
        let cors = observed.cors_cache();
        assert!(cors["live_bytes"].as_u64().unwrap() <= cors["limit_bytes"].as_u64().unwrap());
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
    let publishing = if copy {
        fixture("http-copy-lose-reply").await?;
        let publishing = tokio::spawn(fixture("http-copy-replay"));
        let replaced = path.parent().unwrap().join("copy-source-replaced");
        tokio::time::timeout(Duration::from_secs(15), async {
            while !replaced.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Some(publishing)
    } else if objects {
        fixture("http-objects-lose-reply").await?;
        let publishing = tokio::spawn(fixture("http-objects-replay"));
        let replaced = path.parent().unwrap().join("objects-replaced");
        tokio::time::timeout(Duration::from_secs(15), async {
            while !replaced.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Some(publishing)
    } else if writes || raw {
        fixture(if raw {
            "http-raw-lose-commit"
        } else {
            "chunks-lose-publish"
        })
        .await?;
        let publishing = tokio::spawn(fixture(if raw {
            "http-raw-replay"
        } else {
            "http-writes-publish-replay"
        }));
        let point = path.parent().unwrap().join(if raw {
            "faults-a/node-raw-committed.hit"
        } else {
            "faults-a/node-write-published.hit"
        });
        tokio::time::timeout(Duration::from_secs(15), async {
            while !point.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Some(publishing)
    } else {
        None
    };
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    if let Some(publishing) = publishing {
        publishing.await??;
        fixture(if copy {
            "http-copy-resume-reply"
        } else if objects {
            "http-objects-resume-reply"
        } else if raw {
            "http-raw-resume-commit"
        } else {
            "chunks-resume-publish"
        })
        .await?;
    }
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    if copy {
        fixture("http-copy-reconnected").await?;
    } else if objects {
        fixture("http-objects-reconnected").await?;
        assert_eq!(reads.backend_gets(), 0);
    } else if catalog {
        fixture("http-catalog-reconnected").await?;
        assert_eq!(reads.backend_gets(), 0);
    } else if raw {
        fixture("http-raw-reconnected").await?;
    } else if writes {
        fixture("http-writes-reconnected").await?;
    } else if sources {
        fixture("sources-reconnected").await?;
    } else if observations {
        fixture("observations-reconnected").await?;
    } else if public {
        let before = reads.backend_gets();
        fixture("public-reconnected").await?;
        assert_eq!(reads.backend_gets(), before);
    } else if disk {
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
    if copy {
        let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
        let point = faults.join("node-copy-before-publish");
        tokio::fs::write(&point, b"pause").await?;
        let fenced = tokio::spawn(fixture("http-copy-fenced"));
        tokio::time::timeout(Duration::from_secs(15), async {
            while !point.with_extension("hit").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        context.stop();
        fenced.await??;
        tokio::fs::remove_file(&point).await?;
        driver.await??;
        s3_server.abort();
        web_server.abort();
        let _ = s3_server.await;
        let _ = web_server.await;
        return Ok(());
    }
    if catalog || objects {
        let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
        let point = faults.join("node-catalog-prepared");
        tokio::fs::write(&point, b"pause").await?;
        let fenced = tokio::spawn(fixture(if objects {
            "http-objects-fenced"
        } else {
            "http-catalog-fenced"
        }));
        tokio::time::timeout(Duration::from_secs(15), async {
            while !point.with_extension("hit").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        context.stop();
        tokio::fs::remove_file(&point).await?;
        fenced.await??;
        driver.await??;
        s3_server.abort();
        web_server.abort();
        let _ = s3_server.await;
        let _ = web_server.await;
        return Ok(());
    }
    if writes || raw {
        let before = observed.write_resources();
        let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
        let point = faults.join(if raw {
            "node-raw-uploaded"
        } else {
            "node-write-seal"
        });
        tokio::fs::write(&point, b"pause").await?;
        let fenced = tokio::spawn(fixture(if raw {
            "http-raw-fenced"
        } else {
            "http-writes-fenced"
        }));
        tokio::time::timeout(Duration::from_secs(15), async {
            while !point.with_extension("hit").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        let held = observed.write_resources();
        assert!(
            if raw {
                held[0] < before[0] && held[1] == before[1] && held[2] < before[2]
            } else {
                held.iter().zip(before).all(|(held, before)| *held < before)
            },
            "writer did not hold its resources"
        );
        context.stop();
        fenced.await??;
        if !raw {
            assert_eq!(
                observed.write_resources(),
                held,
                "cancelled blocking work released its resources early"
            );
        }
        tokio::fs::remove_file(&point).await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while observed.write_resources() != before {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        driver.await??;
        s3_server.abort();
        web_server.abort();
        let _ = s3_server.await;
        let _ = web_server.await;
        return Ok(());
    }
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
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    tokio::fs::write(faults.join("node-observations-before-batch"), b"pause").await?;
    fixture("l1-derived").await?;
    assert_eq!(reads.backend_gets(), before);
    tokio::fs::remove_file(faults.join("node-observations-before-batch")).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while reads.node.observations.compression.snapshot()["written"] == 0
            || reads.node.observations.compression.snapshot()["pending"] != 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    fixture("l1-learned").await?;
    assert!(
        reads.backend_gets() > before,
        "learning reused an obsolete object proof"
    );
    let before = reads.backend_gets();
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
