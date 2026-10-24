use super::*;
use crate::node::reads::{Prepared, Reads};

async fn collect(reads: &Arc<Reads>, prepared: Prepared) -> Result<Vec<u8>> {
    let mut stream = reads.body(prepared);
    let mut bytes = Vec::new();
    while let Some(part) = stream.next().await {
        bytes.extend_from_slice(&part?);
        // Give optional digest jobs scheduling time; saturation is covered separately.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(bytes)
}

async fn checked(reads: &Reads) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while reads.read_checks()["active"] != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(reads.read_checks()["used_bytes"], 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated controls, worker, TLS origins and corruptible S3 objects"]
async fn node_checks_verify_bodies_and_report_owned_sources() -> Result<()> {
    use crate::{
        config::node::NodeConfig,
        node::{NodeContext, Phase},
    };
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
    let directory = path.parent().unwrap().join("checks-runtime");
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
    let reads = Arc::new(Reads::new(context.clone())?);
    let input = request(&snapshot.lease.run, &source, "object", &Held::new());
    let prepared = reads.open(input.clone()).await?;
    let mapping: read::Mapping = prepared.material(prepared.reply.mapping.as_ref().unwrap())?;
    assert!(mapping.segments.len() >= 2);
    let grant = |r#type: wire::GrantType| {
        prepared
            .reply
            .grants
            .iter()
            .find(|grant| grant.r#type == r#type as i32)
            .unwrap()
            .id
            .clone()
    };
    let report = wire::ReadReportRequest {
        run: Some(snapshot.lease.run.clone()),
        read_id: grant(wire::GrantType::ReadProtection),
        window_id: grant(wire::GrantType::ReadWindow),
        segments: vec![0, 1],
    };
    for mutation in 0..6 {
        let mut invalid = report.clone();
        let expected = match mutation {
            0 => {
                invalid.segments.clear();
                Code::InvalidArgument
            }
            1 => {
                invalid.segments = vec![0, 0];
                Code::InvalidArgument
            }
            2 => {
                invalid.segments = vec![read::MAX_ITEMS as u32 - 1];
                Code::InvalidArgument
            }
            3 => {
                invalid.window_id = Uuid::new_v4().to_string();
                Code::FailedPrecondition
            }
            4 => {
                invalid.read_id = Uuid::new_v4().to_string();
                Code::FailedPrecondition
            }
            _ => {
                invalid.run.as_mut().unwrap().fence = Uuid::new_v4().to_string();
                Code::FailedPrecondition
            }
        };
        assert_eq!(
            context
                .client
                .report_read(invalid)
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    let worker: NodeState = serde_json::from_slice(&tokio::fs::read(&source.worker_path).await?)?;
    assert_eq!(
        dns.client(&url, worker)?
            .report_read(report.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let tasks = context.client.report_read(report.clone()).await?.task_ids;
    assert_eq!(tasks.len(), 2);
    assert_eq!(
        context.client.report_read(report.clone()).await?.task_ids,
        tasks
    );
    tokio::fs::write(
        path.parent().unwrap().join("check-tasks.json"),
        serde_json::to_vec(&tasks)?,
    )
    .await?;
    fixture("checks-queued").await?;
    // Reconnect before retrying the committed report; its task identities remain stable.
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 3)]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > initial.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    assert_eq!(
        context.client.report_read(report.clone()).await?.task_ids,
        tasks
    );
    drop(prepared);
    fixture("assert-no-reads").await?;
    assert_eq!(
        context.client.report_read(report).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    fixture("checks-run-healthy").await?;
    let prepared = reads.open(input).await?;
    assert_eq!(
        collect(&reads, prepared).await?,
        tokio::fs::read(&source.body_path).await?
    );
    fixture("checks-background").await?;
    let prepared = reads
        .open(request(&snapshot.lease.run, &source, "other", &Held::new()))
        .await?;
    assert_eq!(
        collect(&reads, prepared).await?,
        tokio::fs::read(&source.other_path).await?
    );
    checked(&reads).await?;
    assert!(reads.read_checks()["verified"].as_u64().unwrap() > 0);
    for case in source
        .body_cases
        .iter()
        .filter(|case| case.source != "empty")
    {
        let mut request = request(&snapshot.lease.run, &source, &case.key, &Held::new());
        request.access_key = case.access_key.clone();
        let wire::read_route::Route::Bucket(route) =
            request.route.as_mut().unwrap().route.as_mut().unwrap()
        else {
            unreachable!()
        };
        route.name = case.bucket.clone();
        let prepared = reads.open(request).await?;
        assert_eq!(
            collect(&reads, prepared).await?,
            tokio::fs::read(&case.body_path).await?
        );
        checked(&reads).await?;
    }
    assert_eq!(reads.read_checks()["failed"], 0);
    let raw = source
        .body_cases
        .iter()
        .find(|case| case.source == "raw")
        .unwrap();
    let mut raw_request = request(&snapshot.lease.run, &source, &raw.key, &Held::new());
    raw_request.access_key = raw.access_key.clone();
    let wire::read_route::Route::Bucket(route) =
        raw_request.route.as_mut().unwrap().route.as_mut().unwrap()
    else {
        unreachable!()
    };
    route.name = raw.bucket.clone();
    let mut range = raw_request.clone();
    range.headers.push(wire::ReadHeader {
        name: "range".into(),
        value: b"bytes=11-110".to_vec(),
    });
    let expected = tokio::fs::read(&raw.body_path).await?;
    let prepared = reads.open(range).await?;
    assert_eq!(collect(&reads, prepared).await?, expected[11..111]);
    checked(&reads).await?;
    fixture("checks-range-verified").await?;
    fixture("checks-corrupt-raw").await?;
    let prepared = reads.open(raw_request.clone()).await?;
    let returned = collect(&reads, prepared).await?;
    let expected = tokio::fs::read(&raw.body_path).await?;
    assert_eq!(returned.len(), expected.len());
    assert_ne!(returned, expected);
    checked(&reads).await?;
    assert!(reads.read_checks()["failed"].as_u64().unwrap() > 0);
    fixture("checks-quarantined-raw").await?;
    let before = reads.backend_gets();
    let unavailable = reads.open(raw_request).await?;
    assert_ne!(unavailable.reply.outcome, wire::ReadOutcome::Ready as i32);
    assert!(unavailable.reply.grants.is_empty());
    assert_eq!(reads.backend_gets(), before);
    drop(unavailable);
    fixture("checks-corrupt-chunk").await?;
    let prepared = reads
        .open(request(
            &snapshot.lease.run,
            &source,
            "object",
            &Held::new(),
        ))
        .await?;
    assert!(collect(&reads, prepared).await.is_err());
    fixture("checks-quarantined-chunk").await?;
    checked(&reads).await?;
    fixture("assert-no-reads").await?;
    context.stop();
    driver.await??;
    println!(
        "PASS bounded background digests, owned reports, batching, retry recovery, healthy-source verification and raw/chunk quarantine"
    );
    Ok(())
}
