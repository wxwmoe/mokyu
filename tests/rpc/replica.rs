use super::*;

#[tokio::test]
#[ignore = "requires isolated controls, a physical PostgreSQL standby and controlled replay"]
async fn node_read_replicas_validate_markers_and_fall_back() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let identity: NodeState =
        serde_json::from_slice(&tokio::fs::read(std::env::var("MOKYU_TEST_NODE_STATE")?).await?)?;
    let setup: Fixture =
        serde_json::from_slice(&tokio::fs::read(std::env::var("MOKYU_TEST_READ_FIXTURE")?).await?)?;
    let expected = tokio::fs::read(&setup.body_path).await?;
    let dns = Dns::new().await?;
    *dns.answers.write().unwrap() = Some(vec![Ipv4Addr::new(127, 0, 0, 2)]);
    let client = dns.client(&std::env::var("MOKYU_TEST_CONTROL_URL")?, identity)?;
    let boot = Uuid::new_v4();
    let mut session = client.open(boot, Uuid::new_v4(), 0).await?;
    let run = session.welcome.run.clone().unwrap();
    client.sync(run.clone(), vec![]).await?;
    fixture("replica-setup").await?;
    let mut sequence = 1;
    for case in [
        "ready",
        "signed",
        "lag",
        "recovered",
        "timeout",
        "denied",
        "restored",
        "source",
        "revision",
        "peer",
    ] {
        if case == "peer" {
            *dns.answers.write().unwrap() = Some(vec![
                Ipv4Addr::new(127, 0, 0, 2),
                Ipv4Addr::new(127, 0, 0, 3),
            ]);
            dns.resolver.clear_cache();
            client.addresses.lock().await.refresh_at = Instant::now();
            let epoch = session.welcome.epoch;
            drop(session);
            fixture("stop-first").await?;
            session = client.open(boot, Uuid::new_v4(), epoch).await?;
            assert_eq!(session.welcome.run.as_ref(), Some(&run));
            sequence = 1;
        }
        fixture(&format!("replica-{case}-before")).await?;
        let mut held = Held::new();
        let mut input = request(&run, &setup, "object", &held);
        if case != "signed" {
            input.access_key.clear();
        }
        input.max_items = 1;
        let mut page = client.read(input).await?;
        assert!(page.next.is_some());
        let protection = grant(&page, wire::GrantType::ReadProtection);
        fixture(&format!("replica-{case}-opened")).await?;
        let mut bytes = Vec::new();
        let mut first = true;
        loop {
            apply(&page, &mut held)?;
            bytes.extend(payload(&page, &held).await?);
            let Some(cursor) = page.next.clone() else {
                break;
            };
            let input = wire::ReadWindowRequest {
                run: Some(run.clone()),
                cursor: Some(cursor),
                cached: held.values().map(|(version, _)| version.clone()).collect(),
                max_items: 1,
                ..Default::default()
            };
            if first && matches!(case, "source" | "revision") {
                let error =
                    tokio::time::timeout(Duration::from_secs(2), client.read_window(input.clone()))
                        .await?
                        .expect_err(
                            "stale replica overrode the primary source or payload revision",
                        );
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                fixture(&format!("replica-{case}-repair")).await?;
            }
            first = false;
            let next = client.read_window(input.clone()).await?;
            if case == "ready" {
                let replay = client.read_window(input).await?;
                assert_eq!(next.mapping, replay.mapping);
                assert_eq!(
                    next.grants
                        .iter()
                        .map(|value| (&value.id, value.r#type))
                        .collect::<Vec<_>>(),
                    replay
                        .grants
                        .iter()
                        .map(|value| (&value.id, value.r#type))
                        .collect::<Vec<_>>()
                );
            }
            session
                .pulse_resources(
                    sequence,
                    false,
                    vec![protection.clone()],
                    vec![grant(&page, wire::GrantType::ReadWindow)],
                )
                .await?;
            sequence += 1;
            page = next;
        }
        assert_eq!(bytes, expected, "replica case {case} changed the payload");
        session
            .pulse_resources(sequence, false, Vec::new(), vec![protection])
            .await?;
        sequence += 1;
        fixture(&format!("replica-{case}-after")).await?;
    }
    fixture("assert-no-reads").await?;
    println!(
        "PASS replica markers, signed isolation, lag, timeout, privileges, primary source guards, revisions and peer recovery"
    );
    Ok(())
}
