use super::*;

#[tokio::test]
#[ignore = "requires two isolated controls, a worker identity and S3; see tests/README.md"]
async fn node_work_batches_recover_results_and_preserve_read_pins() -> Result<()> {
    use crate::{
        config::node::NodeConfig,
        node::{NodeContext, Phase, work::Worker},
    };
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    assert_eq!(identity.role, "worker");
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let directory = path.parent().unwrap().join("work-runtime");
    tokio::fs::create_dir(&directory).await?;
    crate::cluster::identity::write_state(&directory.join("node.json"), &identity).await?;
    let config = format!(
        "[instance]\ncontrol_url='{url}'\ndata_path='{}'\n",
        directory.display()
    );
    let mut context = NodeContext::new(NodeConfig::parse(&config, "worker")?, "worker").await?;
    let dns = Dns::new().await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 2)]);
    Arc::get_mut(&mut context).unwrap().client = dns.client(&url, identity)?;
    let mut states = context.subscribe();
    let current = context.clone();
    let driver = tokio::spawn(async move {
        let result = current.run().await;
        if let Err(error) = &result {
            eprintln!("work session failed: {error:#}");
        }
        result
    });
    node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    assert_eq!(context.budget.get().unwrap().database_connections, 0);
    let busy = Worker::new(context.clone())?;
    let capacity = busy.block_reads().await?;
    fixture("work-busy-seed").await?;
    let running = tokio::spawn(busy.run());
    fixture("work-busy-check").await?;
    drop(capacity);
    fixture("work-busy-recovered").await?;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    fixture("lose-work").await?;
    fixture("work-seed").await?;
    let mut worker = tokio::spawn(Worker::new(context.clone())?.run());
    fixture("work-wait-commit").await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 3)]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    fixture("work-check").await?;
    fixture("inspection-seed").await?;
    fixture("inspection-verified").await?;
    fixture("inspection-recheck").await?;
    fixture("inspection-cleared").await?;
    fixture("inspection-namespace").await?;
    fixture("inspection-namespace-restored").await?;
    for action in [
        "audit-metadata",
        "audit-head",
        "audit-full",
        "audit-clean",
        "audit-missing",
        "audit-restored",
    ] {
        for bucket in ["objects", "packed", "raw"] {
            fixture(&format!("{action}-{bucket}")).await?;
        }
    }
    fixture("audit-namespace").await?;
    fixture("sweep-preview").await?;
    fixture("sweep-start").await?;
    fixture("sweep-deleted").await?;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    fixture("sweep-restart").await?;
    worker = tokio::spawn(Worker::new(context.clone())?.run());
    fixture("sweep-check").await?;
    fixture("sweep-namespace").await?;
    context.stop();
    tokio::time::timeout(Duration::from_secs(10), driver).await???;
    tokio::time::timeout(Duration::from_secs(10), worker).await???;
    println!(
        "PASS API-only worker: session wake, bounded 64-item GC, lost committed reply, origin failover, raw multipart abort, read-pin and backend-age protection"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated active raw transfer and checkpoint fixture; see tests/README.md"]
async fn node_raw_checkpoints_require_current_runs_and_permits() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let identity: NodeState =
        serde_json::from_slice(&tokio::fs::read(std::env::var("MOKYU_TEST_NODE_STATE")?).await?)?;
    let value: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(std::env::var("MOKYU_TEST_TRANSFER_CHECKPOINT")?).await?,
    )?;
    let string = |name: &str| {
        value[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("checkpoint field missing"))
    };
    let input = wire::TransferCheckpointRequest {
        run: Some(wire::Run {
            boot_id: string("boot")?,
            fence: string("fence")?,
        }),
        operation_id: string("operation")?,
        progress_json: serde_json::to_vec(&value["progress"])?,
    };
    let dns = Dns::new().await?;
    let client = dns.client(&std::env::var("MOKYU_TEST_CONTROL_URL")?, identity)?;
    client.checkpoint_transfer(input.clone()).await?;
    client.checkpoint_transfer(input.clone()).await?;
    let mut stale = input.clone();
    stale.run.as_mut().unwrap().fence = Uuid::new_v4().to_string();
    assert_eq!(
        client.checkpoint_transfer(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut unknown = input.clone();
    unknown.operation_id = Uuid::now_v7().to_string();
    assert_eq!(
        client
            .checkpoint_transfer(unknown)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut invalid = input.clone();
    invalid.progress_json = br#"{"action":"part","upload":null,"number":0,"etag":"bad"}"#.to_vec();
    assert_eq!(
        client
            .checkpoint_transfer(invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    for (action, expected) in [
        ("checkpoint-expired-run", Code::FailedPrecondition),
        ("checkpoint-expired-permit", Code::FailedPrecondition),
        ("checkpoint-disabled", Code::PermissionDenied),
        ("checkpoint-maintenance", Code::Aborted),
    ] {
        fixture(action).await?;
        let response = client.checkpoint_transfer(input.clone()).await;
        fixture("checkpoint-restore").await?;
        assert_eq!(response.unwrap_err().code(), expected, "{action}");
        client.checkpoint_transfer(input.clone()).await?;
    }
    Ok(())
}
