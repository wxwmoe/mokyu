use super::*;
use crate::{
    config::node::NodeConfig,
    node::{NodeContext, Phase, reads::Reads},
};

#[tokio::test]
#[ignore = "requires isolated controls, S3, worker mutations and source lifetime checks"]
async fn node_read_snapshots_survive_mutations_and_gc() -> Result<()> {
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
    let directory = path.parent().unwrap().join("snapshot-runtime");
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
    fixture("snapshots-setup").await?;
    let reads = Arc::new(Reads::new(context.clone())?);
    let packed = source
        .body_cases
        .iter()
        .find(|case| case.source == "pack")
        .context("packed fixture missing")?;
    let raw = source
        .body_cases
        .iter()
        .find(|case| case.source == "raw")
        .context("raw fixture missing")?;
    for case in [
        "overwrite",
        "raw-overwrite",
        "rewrite",
        "pack-rewrite",
        "unpack",
        "repack",
        "permission",
    ] {
        fixture(&format!("snapshots-{case}-before")).await?;
        let is_pack = matches!(case, "pack-rewrite" | "unpack" | "repack");
        let fixture_case = if is_pack {
            Some(packed)
        } else if case == "raw-overwrite" {
            Some(raw)
        } else {
            None
        };
        let key = if let Some(fixture) = fixture_case {
            &fixture.key
        } else if case == "overwrite" {
            "object"
        } else {
            "other"
        };
        let mut input = request(&context.snapshot()?.lease.run, &source, key, &Held::new());
        let expected = tokio::fs::read(if let Some(fixture) = fixture_case {
            &fixture.body_path
        } else if case == "overwrite" {
            &source.body_path
        } else {
            &source.other_path
        })
        .await?;
        if let Some(fixture) = fixture_case {
            input.access_key = fixture.access_key.clone();
            let Some(wire::read_route::Route::Bucket(route)) =
                input.route.as_mut().and_then(|route| route.route.as_mut())
            else {
                unreachable!()
            };
            route.name = fixture.bucket.clone();
        }
        let old = reads.open(input.clone()).await?;
        assert!(old.reply.next.is_none());
        let old_mapping: read::Mapping = old.material(old.reply.mapping.as_ref().unwrap())?;
        let old_object = old.reply.object.clone();
        fixture(&format!("snapshots-{case}-mutate")).await?;
        if case == "repack" {
            *dns.answers.write().unwrap() = Some(vec![
                Ipv4Addr::new(127, 0, 0, 2),
                Ipv4Addr::new(127, 0, 0, 3),
            ]);
            dns.resolver.clear_cache();
            context.client.addresses.lock().await.refresh_at = Instant::now();
            fixture("stop-first").await?;
            node_state(&mut states, |state| {
                state.phase == Phase::Ready && state.epoch > initial.epoch
            })
            .await?;
            assert!(Arc::ptr_eq(&old.run, &context.snapshot()?.lease));
        }
        input.operation_id = Uuid::new_v4().to_string();
        let new = reads.open(input).await?;
        if case == "permission" {
            assert_eq!(new.reply.outcome, wire::ReadOutcome::AccessDenied as i32);
            assert!(new.reply.mapping.is_none() && new.reply.grants.is_empty());
            drop(new);
        } else {
            assert_eq!(new.reply.outcome, wire::ReadOutcome::Ready as i32);
            let new_mapping: read::Mapping = new.material(new.reply.mapping.as_ref().unwrap())?;
            assert_ne!(
                serde_json::to_value(&old_mapping)?,
                serde_json::to_value(&new_mapping)?,
                "mutation did not replace the selected sources: {case}"
            );
            if case.ends_with("overwrite") {
                assert_ne!(old_object, new.reply.object);
            }
            if case.ends_with("rewrite") {
                let old_keys: std::collections::HashSet<_> = old_mapping
                    .segments
                    .iter()
                    .filter_map(|segment| segment.source.key())
                    .collect();
                let new_keys: std::collections::HashSet<_> = new_mapping
                    .segments
                    .iter()
                    .filter_map(|segment| segment.source.key())
                    .collect();
                assert!(
                    !old_keys.is_empty() && new_keys.len() == 1 && old_keys.is_disjoint(&new_keys)
                );
            }
            let mut bytes = Vec::new();
            let mut body = reads.body(new);
            while let Some(part) = body.next().await {
                bytes.extend_from_slice(&part?);
            }
            let current = if case.ends_with("overwrite") {
                tokio::fs::read(&source.other_path).await?
            } else {
                expected.clone()
            };
            assert_eq!(bytes, current, "current read returned wrong bytes: {case}");
        }
        fixture(&format!("snapshots-{case}-protected")).await?;
        let mut bytes = Vec::new();
        let mut body = reads.body(old);
        while let Some(part) = body.next().await {
            bytes.extend_from_slice(&part?);
        }
        assert_eq!(
            bytes, expected,
            "held read returned mixed or unavailable bytes: {case}"
        );
        drop(body);
        fixture("assert-no-reads").await?;
        fixture(&format!("snapshots-{case}-released")).await?;
    }
    context.stop();
    driver.await??;
    println!(
        "PASS held read snapshots across overwrite, key rewrite, unpack, repack, authorization changes and physical GC"
    );
    Ok(())
}
