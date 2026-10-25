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
    fixture("lose-work").await?;
    fixture("work-seed").await?;
    let worker = tokio::spawn(Worker::new(context.clone()).run());
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
    context.stop();
    tokio::time::timeout(Duration::from_secs(10), driver).await???;
    tokio::time::timeout(Duration::from_secs(10), worker).await???;
    println!(
        "PASS API-only worker: session wake, bounded 64-item GC, lost committed reply, origin failover, raw multipart abort, read-pin and backend-age protection"
    );
    Ok(())
}
