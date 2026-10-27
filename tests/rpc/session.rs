use super::*;
use crate::model::{node::NodeReport, node_pack, node_work};
use serde_json::json;

#[tokio::test]
#[ignore = "requires two isolated controls and TLS origins; see tests/README.md"]
async fn node_sessions_push_reports_and_close() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let path = PathBuf::from(std::env::var("MOKYU_TEST_NODE_STATE")?);
    let identity: NodeState = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let dns = Dns::new().await?;
    let client = dns.client(&std::env::var("MOKYU_TEST_CONTROL_URL")?, identity)?;
    let boot = Uuid::new_v4();
    let connection = Uuid::new_v4();
    let mut session = client.open(boot, connection, 0).await?;
    assert_eq!(session.peer, Ipv4Addr::new(127, 0, 0, 2));
    let run = session.welcome.run.clone().unwrap();
    let control = session.welcome.control_id.clone();
    let deadline = session.valid_until;
    let acknowledged = session.acknowledged_at;
    fixture("change-settings").await?;
    tokio::time::timeout(Duration::from_secs(3), session.notified()).await??;
    assert_eq!(session.valid_until, deadline);
    assert_eq!(session.acknowledged_at, acknowledged);
    let state = session.pulse(1, false).await?;
    let changed = client.sync(run.clone(), vec![]).await?;
    assert_eq!(state.revisions, changed.state.unwrap().revisions);
    assert!(
        changed
            .settings
            .iter()
            .any(|setting| setting.json.windows(3).any(|s| s == b"31s"))
    );
    fixture("session-task-notice").await?;
    tokio::time::timeout(Duration::from_secs(3), session.notified()).await??;

    let mut report = NodeReport {
        boot_id: boot,
        sequence: 1,
        sampled_at: chrono::Utc::now(),
        metadata: Some(json!({"format": 1, "diagnostics": "x".repeat(50 * 1024)})),
        status: json!({"work":"normal", "diagnostics":"x".repeat(20 * 1024),
            "runtime":{"started_at":chrono::Utc::now(),"uptime_seconds":10}}),
    };
    let request = |report: &NodeReport| -> Result<wire::NodeReportRequest> {
        Ok(wire::NodeReportRequest {
            run: Some(run.clone()),
            report_json: serde_json::to_vec(report)?,
            sample_interval_seconds: 10,
        })
    };
    let input = request(&report)?;
    assert!(prost::Message::encoded_len(&input) > SESSION_LIMIT);
    assert!(client.report(input.clone()).await?);
    assert!(!client.report(input).await?);
    session.pulse(2, false).await?;
    fixture("session-check-report").await?;
    report.sequence = 2;
    report.metadata = None;
    assert!(client.report(request(&report)?).await?);
    report.sequence = 3;
    report.status["diagnostics"] = json!("x".repeat(31 * 1024));
    assert_eq!(
        client.report(request(&report)?).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    let completion = node_work::Completion {
        operation: Uuid::now_v7(),
        outcomes: vec![node_work::Outcome::Packed(node_pack::Encoded {
            chunks: vec![u32::MAX; 4096],
            packs: (0..2048).map(|i| (i, u32::MAX, "x".repeat(44))).collect(),
        })],
    };
    let mut input = wire::WorkRequest {
        run: Some(run.clone()),
        operation_id: Uuid::now_v7().to_string(),
        completion_json: Some(serde_json::to_vec(&completion)?),
        class: 1,
        max_items: 64,
        cached: vec![],
    };
    assert!(prost::Message::encoded_len(&input) > crate::rpc::read::REQUEST_LIMIT);
    assert_eq!(
        client
            .exchange_work(input.clone())
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    input.completion_json = Some(serde_json::to_vec(&node_work::Completion {
        operation: completion.operation,
        outcomes: vec![node_work::Outcome::Inspected {
            code: Some("x".repeat(140 * 1024)),
        }],
    })?);
    assert_eq!(
        client.exchange_work(input).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    client.cursor.store(1, Ordering::Relaxed);
    let mut replaced = client.open(boot, connection, 0).await?;
    assert_ne!(replaced.welcome.control_id, control);
    assert_eq!(
        session.pulse(3, false).await.unwrap_err().code(),
        Code::Aborted
    );
    client.close(run.clone()).await?;
    client.close(run.clone()).await?;
    assert_eq!(
        replaced.pulse(1, false).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .open(boot, connection, replaced.welcome.epoch)
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let mut rebooted = client.open(Uuid::new_v4(), Uuid::new_v4(), 0).await?;
    assert_eq!(
        client.close(run).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    rebooted.pulse(1, false).await?;

    // Idle time counts toward the acknowledgement deadline, even if the peer is healthy.
    rebooted.acknowledged_at = Instant::now() - Duration::from_secs(31);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), rebooted.pulse(2, false))
            .await?
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    let run = rebooted.welcome.run.clone().unwrap();
    let epoch = rebooted.welcome.epoch;
    drop(rebooted);
    let mut rebooted = client
        .open(Uuid::parse_str(&run.boot_id)?, Uuid::new_v4(), epoch)
        .await?;
    rebooted.pulse(1, false).await?;

    // Unsolicited notices cannot keep a silent client alive.
    fixture("session-notice-flood").await?;
    let start = Instant::now();
    let mut notices = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(35), rebooted.notified()).await? {
            Ok(()) => notices += 1,
            Err(status) => {
                assert_eq!(status.code(), Code::DeadlineExceeded);
                break;
            }
        }
        ensure!(
            start.elapsed() < Duration::from_secs(35),
            "notices extended the heartbeat deadline"
        );
    }
    assert!((1..=32).contains(&notices));
    println!(
        "PASS cross-control notifications, bounded reports, heartbeat isolation, large pack results, closed-run fencing and notice flood deadline ({notices} notices)"
    );
    Ok(())
}
