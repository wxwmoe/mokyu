use super::*;
use crate::config::node::NodeConfig;
use crate::node::{NodeContext, Phase};

#[tokio::test]
#[ignore = "requires isolated TLS origins with keepalive_requests 8"]
async fn node_rpc_survives_connection_rotation() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let directory = path.parent().unwrap().join("rotation-runtime");
    tokio::fs::create_dir(&directory).await?;
    crate::cluster::identity::write_state(&directory.join("node.json"), &identity).await?;
    let config = format!(
        "[instance]\ncontrol_url='{url}'\ndata_path='{}'\n",
        directory.display()
    );
    let mut context = NodeContext::new(NodeConfig::parse(&config, "data")?, "data").await?;
    let dns = Dns::new().await?;
    Arc::get_mut(&mut context).unwrap().client = dns.client(&url, identity)?;
    let mut states = context.subscribe();
    let driver = tokio::spawn(context.clone().run());
    node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    for _ in 0..100 {
        let run = context.snapshot()?.lease.run.clone();
        for result in
            futures_util::future::join_all((0..8).map(|_| context.client.sync(run.clone(), vec![])))
                .await
        {
            result.inspect_err(|error| {
                eprintln!(
                    "rotation failure: retryable={}, transport_closed={}, metrics={}",
                    retryable(error),
                    transport_closed(error),
                    context.client.statistics()
                );
            })?;
        }
    }
    context.stop();
    tokio::time::timeout(Duration::from_secs(10), driver).await???;
    println!("PASS concurrent RPCs across repeated TLS origin connection rotations");
    Ok(())
}
