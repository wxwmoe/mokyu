use super::*;
use crate::{
    config::node::NodeConfig,
    node::{NodeContext, Phase},
};

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and write admission checks"]
async fn node_write_admission_recovers_replies_and_renews_over_sessions() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let source: Fixture =
        serde_json::from_slice(&tokio::fs::read(std::env::var("MOKYU_TEST_READ_FIXTURE")?).await?)?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let directory = path.parent().unwrap().join("write-runtime");
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
    let initial = node_state(&mut states, |state| state.phase == Phase::Ready).await?;
    let snapshot = context.snapshot()?;
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    fixture("writes-lose-reply").await?;
    context.client.cursor.store(0, Ordering::Relaxed);
    let mut input = wire::BeginWriteRequest {
        run: Some(snapshot.lease.run.clone()),
        operation_id: Uuid::now_v7().to_string(),
        access_key: source.access_key.clone(),
        bucket: source.bucket,
        key: "rpc-write".into(),
        metadata_json: br#"{"content_type":"text/plain"}"#.to_vec(),
        public_read: false,
        size: Some(10),
        cached: vec![],
    };
    let started = Instant::now();
    let reply = context.client.begin_write(input.clone()).await?;
    assert_eq!(reply.phase, wire::WritePhase::WriteActive as i32);
    assert_eq!(reply.materials.len(), 1);
    assert_eq!(reply.grants.len(), 1);
    assert_eq!(
        reply.grants[0].r#type,
        wire::GrantType::WriteProtection as i32
    );
    let held = context
        .resources
        .adopt(&snapshot.lease, &reply.grants, started)?;
    fixture("writes-resume-reply").await?;
    input.cached = reply
        .materials
        .iter()
        .filter_map(|m| m.version.clone())
        .collect();
    let replay = context.client.begin_write(input.clone()).await?;
    assert_eq!(replay.stream_id, reply.stream_id);
    assert_eq!(replay.grants[0].id, reply.grants[0].id);
    assert!(matches!(
        replay.materials[0].value,
        Some(wire::material::Value::Reuse(true))
    ));
    fixture("writes-single-admission").await?;
    let lookup = wire::WriteRequest {
        run: input.run.clone(),
        operation_id: input.operation_id.clone(),
    };
    let status = context.client.write_status(lookup.clone()).await?;
    assert_eq!(status.stream_id, reply.stream_id);
    assert!(status.materials.is_empty() && status.grants.is_empty());
    let mut conflict = input.clone();
    conflict.size = Some(11);
    assert_eq!(
        context
            .client
            .begin_write(conflict)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut invalid = input.clone();
    invalid.operation_id = Uuid::new_v4().to_string();
    assert_eq!(
        context
            .client
            .begin_write(invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut denied = input.clone();
    denied.operation_id = Uuid::now_v7().to_string();
    denied.access_key = "unknown-credential".into();
    let denied = context.client.begin_write(denied).await.unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(
        denied.metadata().get("mokyu-s3-code").unwrap(),
        "AccessDenied"
    );
    fixture("writes-record-expiry").await?;
    fixture("writes-check-renewal").await?;
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > initial.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    assert!(Arc::ptr_eq(&context.snapshot()?.lease, &snapshot.lease));
    held[0].check()?;
    assert_eq!(
        context.client.begin_write(input.clone()).await?.stream_id,
        reply.stream_id
    );
    assert_eq!(
        context.client.abort_write(lookup.clone()).await?.phase,
        wire::WritePhase::WriteAborted as i32
    );
    assert_eq!(
        context.client.abort_write(lookup.clone()).await?.phase,
        wire::WritePhase::WriteAborted as i32
    );
    drop(held);
    fixture("writes-check-abort").await?;
    assert_eq!(
        context.client.begin_write(input.clone()).await?.phase,
        wire::WritePhase::WriteAborted as i32
    );
    input.operation_id = Uuid::now_v7().to_string();
    input.key = "rpc-release".into();
    let started = Instant::now();
    let released = context.client.begin_write(input.clone()).await?;
    let held = context
        .resources
        .adopt(&snapshot.lease, &released.grants, started)?;
    drop(held);
    fixture("writes-check-release").await?;
    assert_eq!(
        context
            .client
            .write_status(wire::WriteRequest {
                run: input.run,
                operation_id: input.operation_id
            })
            .await?
            .phase,
        wire::WritePhase::WriteAborted as i32
    );
    context.stop();
    driver.await??;
    println!(
        "PASS idempotent write admission, committed reply loss, cached storage, session renewal, origin recovery, explicit abort and last-owner release without database configuration"
    );
    Ok(())
}
