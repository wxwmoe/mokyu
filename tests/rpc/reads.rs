use super::*;
use crate::{
    content::{codec, compression},
    model::node_read as read,
    storage::model::{Options, Store},
};
use futures_util::StreamExt;
use object_store::{ObjectStoreExt, path::Path};
use std::collections::{BTreeMap, HashMap};

#[path = "http.rs"]
mod http;

#[derive(serde::Deserialize)]
struct Fixture {
    bucket: String,
    access_key: String,
    secret_key: String,
    body_path: PathBuf,
    other_path: PathBuf,
    worker_path: PathBuf,
    #[serde(default)]
    body_cases: Vec<BodyCase>,
}

#[derive(serde::Deserialize)]
struct BodyCase {
    bucket: String,
    key: String,
    access_key: String,
    body_path: PathBuf,
    source: String,
}

type Held = HashMap<(i32, String), (wire::Cached, Vec<u8>)>;

fn apply(reply: &wire::ReadReply, held: &mut Held) -> Result<()> {
    for material in &reply.materials {
        let version = material
            .version
            .as_ref()
            .context("material version missing")?;
        assert_eq!(version.revision.len(), 32);
        let key = (version.r#type, version.id.clone());
        match material.value.as_ref().context("material value missing")? {
            wire::material::Value::Json(json) => {
                held.insert(key, (version.clone(), json.clone()));
            }
            wire::material::Value::Reuse(true) => assert_eq!(
                &held.get(&key).context("reused material was not held")?.0,
                version
            ),
            _ => anyhow::bail!("invalid reuse marker"),
        }
    }
    Ok(())
}

fn get<T: serde::de::DeserializeOwned>(
    held: &Held,
    r#type: wire::MaterialType,
    id: &str,
) -> Result<T> {
    Ok(serde_json::from_slice(
        &held
            .get(&(r#type as i32, id.to_owned()))
            .context("required material missing")?
            .1,
    )?)
}

fn request(run: &wire::Run, fixture: &Fixture, key: &str, held: &Held) -> wire::ReadRequest {
    wire::ReadRequest {
        run: Some(run.clone()),
        operation_id: Uuid::new_v4().to_string(),
        access_key: fixture.access_key.clone(),
        route: Some(wire::ReadRoute {
            route: Some(wire::read_route::Route::Bucket(wire::BucketRoute {
                name: fixture.bucket.clone(),
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
        cached: held.values().map(|(version, _)| version.clone()).collect(),
        ..Default::default()
    }
}

fn grant(reply: &wire::ReadReply, r#type: wire::GrantType) -> wire::GrantRef {
    let grant = reply
        .grants
        .iter()
        .find(|grant| grant.r#type == r#type as i32)
        .unwrap();
    wire::GrantRef {
        r#type: grant.r#type,
        id: grant.id.clone(),
    }
}

async fn payload(reply: &wire::ReadReply, held: &Held) -> Result<Vec<u8>> {
    let mapping: read::Mapping = get(
        held,
        wire::MaterialType::ObjectMapping,
        &reply
            .mapping
            .as_ref()
            .context("mapping reference missing")?
            .id,
    )?;
    let keys: BTreeMap<_, _> = held
        .iter()
        .filter(|((r#type, _), _)| *r#type == wire::MaterialType::BlockKey as i32)
        .map(|(_, (_, json))| {
            let key: read::BlockKey = serde_json::from_slice(json)?;
            Ok((key.id, codec::Key::new(&key.algorithm, key.material)?))
        })
        .collect::<Result<_>>()?;
    let mut stores = HashMap::new();
    let mut result = Vec::new();
    for segment in &mapping.segments {
        let read::Source::Chunk { chunk } = &segment.source else {
            anyhow::bail!("fixture expects standalone chunks");
        };
        let storage_id = chunk.storage_id.context("missing storage identity")?;
        let storage: read::Storage =
            get(held, wire::MaterialType::Storage, &storage_id.to_string())?;
        if let std::collections::hash_map::Entry::Vacant(entry) = stores.entry(storage_id) {
            let store = Store::connect(
                &storage.connection,
                Options::from_profile(&storage.profile)?,
            )?;
            store.ensure_verified().await?;
            entry.insert(store);
        }
        let bytes = stores[&storage_id]
            .backend
            .get(&Path::from(crate::storage::chunk_name(chunk.location_id)))
            .await?
            .bytes()
            .await?;
        let pool = compression::Pool::new(storage.profile.compression_config("30s"), 1)?;
        let (decoded, _) = codec::decode(
            chunk,
            bytes.to_vec(),
            &keys,
            storage.profile.compression_min_savings_percent as u8,
            &pool,
        )?;
        result.extend_from_slice(
            &decoded
                [segment.source_offset as usize..(segment.source_offset + segment.length) as usize],
        );
    }
    assert_eq!(result.len() as i64, mapping.end - mapping.start);
    Ok(result)
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and encrypted S3 objects"]
async fn node_read_resources_renew_reconnect_and_release_automatically() -> Result<()> {
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
    let directory = path.parent().unwrap().join("resource-runtime");
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
    let reads = Arc::new(crate::node::reads::Reads::new(context.clone())?);
    let mut head = request(&snapshot.lease.run, &source, "object", &Held::new());
    head.method = wire::ReadMethod::Head as i32;
    head.include = vec![wire::ReadInclude::Object as i32];
    let first = reads.open(head.clone()).await?;
    let first_object: read::Object = first.material(first.reply.object.as_ref().unwrap())?;
    let second = reads.open(head).await?;
    let second_object: read::Object = second.material(second.reply.object.as_ref().unwrap())?;
    assert_eq!(first_object.id, second_object.id);
    assert_eq!(
        first_object.size,
        tokio::fs::metadata(&source.body_path).await?.len() as i64
    );
    drop((first, second));
    let expected = tokio::fs::read(&source.body_path).await?;
    for range in [None, Some((128usize, expected.len() - 129))] {
        let mut input = request(&snapshot.lease.run, &source, "object", &Held::new());
        input.max_items = 1;
        if let Some((start, end)) = range {
            input.headers.push(wire::ReadHeader {
                name: "range".into(),
                value: format!("bytes={start}-{}", end - 1).into_bytes(),
            });
        }
        let prepared = reads.open(input).await?;
        let mut body = reads.body(prepared);
        let mut received = Vec::new();
        while let Some(bytes) = body.next().await {
            received.extend_from_slice(&bytes?);
        }
        let (start, end) = range.unwrap_or((0, expected.len()));
        assert_eq!(received, expected[start..end]);
    }
    fixture("assert-no-reads").await?;
    for case in &source.body_cases {
        let expected = tokio::fs::read(&case.body_path).await?;
        let spans = if expected.is_empty() {
            vec![None]
        } else {
            vec![None, Some((13, expected.len() - 17))]
        };
        for range in spans {
            let mut input = request(&snapshot.lease.run, &source, &case.key, &Held::new());
            let Some(wire::read_route::Route::Bucket(route)) =
                input.route.as_mut().and_then(|route| route.route.as_mut())
            else {
                unreachable!()
            };
            route.name = case.bucket.clone();
            input.access_key = case.access_key.clone();
            input.max_items = if case.source == "pack" { 0 } else { 1 };
            if let Some((start, end)) = range {
                input.headers.push(wire::ReadHeader {
                    name: "range".into(),
                    value: format!("bytes={start}-{}", end - 1).into_bytes(),
                });
            }
            let prepared = reads.open(input).await?;
            if expected.is_empty() {
                assert!(prepared.reply.mapping.is_none() && prepared.reply.grants.is_empty());
            } else {
                let plan: read::Mapping =
                    prepared.material(prepared.reply.mapping.as_ref().unwrap())?;
                assert!(
                    match case.source.as_str() {
                        "pack" => plan
                            .segments
                            .iter()
                            .any(|segment| matches!(&segment.source, read::Source::Pack { .. })),
                        "raw" => matches!(&plan.segments[0].source, read::Source::Raw { .. }),
                        _ => false,
                    },
                    "fixture did not produce {}",
                    case.source
                );
            }
            let mut body = reads.body(prepared);
            let mut received = Vec::new();
            while let Some(bytes) = body.next().await {
                received.extend_from_slice(&bytes?);
            }
            let (start, end) = range.unwrap_or((0, expected.len()));
            assert_eq!(
                received,
                expected[start..end],
                "{} body mismatch",
                case.source
            );
        }
        fixture("assert-no-reads").await?;
    }
    let faults = PathBuf::from(std::env::var("MOKYU_TEST_FAULT_DIR")?);
    let fault = faults.join("node-read-decode");
    tokio::fs::write(&fault, b"pause").await?;
    let prepared = reads
        .open(request(
            &snapshot.lease.run,
            &source,
            "object",
            &Held::new(),
        ))
        .await?;
    let mut body = reads.body(prepared);
    let waiting = tokio::spawn(async move { body.next().await });
    tokio::time::timeout(Duration::from_secs(15), async {
        while !faults.join("node-read-decode.hit").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    fixture("assert-live-read").await?;
    tokio::fs::remove_file(fault).await?;
    fixture("assert-no-reads").await?;
    *dns.answers.write().unwrap() = Some(vec![
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ]);
    dns.resolver.clear_cache();
    context.client.addresses.lock().await.refresh_at = Instant::now();
    let started = Instant::now();
    let reply = context
        .client
        .read(request(
            &snapshot.lease.run,
            &source,
            "object",
            &Held::new(),
        ))
        .await?;
    let grants = context
        .resources
        .adopt(&snapshot.lease, &reply.grants, started)?;
    assert_eq!(grants.len(), 3);
    let mut held = Held::new();
    apply(&reply, &mut held)?;
    fixture("record-read-expiry").await?;
    tokio::time::sleep(Duration::from_secs(12)).await;
    fixture("assert-read-renewed").await?;
    for grant in &grants {
        grant.check()?;
    }
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > initial.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    assert!(Arc::ptr_eq(&context.snapshot()?.lease, &snapshot.lease));
    for grant in &grants {
        grant.check()?;
    }
    assert_eq!(
        payload(&reply, &held).await?,
        tokio::fs::read(&source.body_path).await?
    );
    drop(grants);
    fixture("assert-no-reads").await?;
    context.stop();
    driver.await??;
    println!(
        "PASS automatic resource renewal, same-run origin recovery, protected S3 read and last-owner release without database configuration"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and encrypted S3 objects"]
async fn node_read_plans_reuse_materials_fetch_s3_and_survive_lost_replies() -> Result<()> {
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
    let other = tokio::fs::read(&setup.other_path).await?;
    let dns = Dns::new().await?;
    let url = std::env::var("MOKYU_TEST_CONTROL_URL")?;
    let client = dns.client(&url, identity)?;
    let boot = Uuid::new_v4();
    let mut session = client.open(boot, Uuid::new_v4(), 0).await?;
    let run = session.welcome.run.clone().unwrap();
    client.sync(run.clone(), vec![]).await?;
    let mut held = Held::new();
    let cold = client.read(request(&run, &setup, "object", &held)).await?;
    assert_eq!(cold.outcome, wire::ReadOutcome::Ready as i32);
    assert!(cold.next.is_none());
    assert!(
        cold.materials
            .iter()
            .any(|m| m.version.as_ref().unwrap().r#type == wire::MaterialType::BlockKey as i32)
    );
    apply(&cold, &mut held)?;
    assert_eq!(
        get::<read::Credential>(&held, wire::MaterialType::S3Credential, &setup.access_key)?.secret,
        setup.secret_key
    );
    // No control call is made between this single OpenRead and the verified S3 payload.
    assert_eq!(payload(&cold, &held).await?, expected);
    let mut cached_body = request(&run, &setup, "object", &held);
    cached_body.body = Some(wire::CachedBody {
        object: cold.object.clone(),
        representation: "identity".into(),
        policy: crate::http::object::POLICY as u32,
        mapping: cold.mapping.clone(),
        storages: cold
            .materials
            .iter()
            .filter_map(|m| {
                m.version
                    .as_ref()
                    .filter(|v| v.r#type == wire::MaterialType::Storage as i32)
                    .cloned()
            })
            .collect(),
        verified: true,
    });
    let cached_result = client.read(cached_body.clone()).await?;
    assert!(
        cached_result.body_reused
            && cached_result.mapping.is_none()
            && cached_result.grants.is_empty()
    );
    let mut sequence = 1;
    session
        .pulse_resources(
            sequence,
            false,
            Vec::new(),
            vec![grant(&cold, wire::GrantType::ReadProtection)],
        )
        .await?;
    sequence += 1;

    fixture("change-read-profile").await?;
    cached_body.operation_id = Uuid::new_v4().to_string();
    let changed_policy = client.read(cached_body).await?;
    assert!(!changed_policy.body_reused && changed_policy.mapping.is_some());
    assert!(
        changed_policy
            .materials
            .iter()
            .any(
                |m| m.version.as_ref().unwrap().r#type == wire::MaterialType::Storage as i32
                    && matches!(m.value, Some(wire::material::Value::Json(_)))
            )
    );
    apply(&changed_policy, &mut held)?;
    assert_eq!(payload(&changed_policy, &held).await?, expected);
    session
        .pulse_resources(
            sequence,
            false,
            Vec::new(),
            vec![grant(&changed_policy, wire::GrantType::ReadProtection)],
        )
        .await?;
    sequence += 1;

    let warm = client.read(request(&run, &setup, "other", &held)).await?;
    for r#type in [
        wire::MaterialType::Bucket,
        wire::MaterialType::Storage,
        wire::MaterialType::BlockKey,
        wire::MaterialType::S3Credential,
    ] {
        assert!(
            warm.materials
                .iter()
                .filter(|m| m.version.as_ref().unwrap().r#type == r#type as i32)
                .all(|m| m.value == Some(wire::material::Value::Reuse(true)))
        );
    }
    apply(&warm, &mut held)?;
    assert_eq!(payload(&warm, &held).await?, other);
    session
        .pulse_resources(
            sequence,
            false,
            Vec::new(),
            vec![grant(&warm, wire::GrantType::ReadProtection)],
        )
        .await?;
    sequence += 1;
    let mut paged = request(&run, &setup, "object", &held);
    paged.max_items = 1;
    let mut page = client.read(paged).await?;
    let protection = grant(&page, wire::GrantType::ReadProtection);
    let mut bytes = Vec::new();
    loop {
        apply(&page, &mut held)?;
        bytes.extend(payload(&page, &held).await?);
        let Some(cursor) = page.next.clone() else {
            break;
        };
        let next = client
            .read_window(wire::ReadWindowRequest {
                run: Some(run.clone()),
                cursor: Some(cursor),
                cached: held.values().map(|(version, _)| version.clone()).collect(),
                max_items: 1,
                ..Default::default()
            })
            .await?;
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
    assert_eq!(bytes, expected);
    session
        .pulse_resources(sequence, false, Vec::new(), vec![protection])
        .await?;
    sequence += 1;
    let mut range = request(&run, &setup, "object", &held);
    range.headers = vec![wire::ReadHeader {
        name: "range".into(),
        value: format!("bytes={}-", expected.len() - 1234).into_bytes(),
    }];
    let range = client.read(range).await?;
    apply(&range, &mut held)?;
    assert!(range.next.is_none());
    assert_eq!(
        payload(&range, &held).await?,
        expected[expected.len() - 1234..]
    );
    session
        .pulse_resources(
            sequence,
            false,
            Vec::new(),
            vec![grant(&range, wire::GrantType::ReadProtection)],
        )
        .await?;
    sequence += 1;

    for r#type in [
        wire::ReadOutcome::NotModified,
        wire::ReadOutcome::InvalidRange,
    ] {
        let mut input = request(&run, &setup, "object", &held);
        input.headers = vec![if r#type == wire::ReadOutcome::NotModified {
            wire::ReadHeader {
                name: "if-none-match".into(),
                value: cold.etag.as_bytes().to_vec(),
            }
        } else {
            wire::ReadHeader {
                name: "range".into(),
                value: b"bytes=99999999999-".to_vec(),
            }
        }];
        let reply = client.read(input).await?;
        assert_eq!(reply.outcome, r#type as i32);
        assert!(reply.grants.is_empty());
    }
    let worker: NodeState = serde_json::from_slice(&tokio::fs::read(&setup.worker_path).await?)?;
    let worker_client = dns.client(&url, worker)?;
    let worker_session = worker_client
        .open(Uuid::new_v4(), Uuid::new_v4(), 0)
        .await?;
    assert_eq!(
        worker_client
            .read(request(
                worker_session.welcome.run.as_ref().unwrap(),
                &setup,
                "object",
                &Held::new()
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    drop(worker_session);

    fixture("lose-read").await?;
    client.cursor.store(0, Ordering::Relaxed);
    let input = request(&run, &setup, "object", &held);
    let recovered = client.read(input.clone()).await?;
    fixture("resume-read").await?;
    let replayed = client.read(input).await?;
    assert_eq!(
        grant(&recovered, wire::GrantType::ReadProtection),
        grant(&replayed, wire::GrantType::ReadProtection)
    );
    apply(&recovered, &mut held)?;
    assert_eq!(payload(&recovered, &held).await?, expected);
    let epoch = session.welcome.epoch;
    drop(session);
    client.cursor.store(1, Ordering::Relaxed);
    session = client.open(boot, Uuid::new_v4(), epoch).await?;
    assert_eq!(session.welcome.run.as_ref(), Some(&run));
    sequence = 1;
    session
        .pulse_resources(
            sequence,
            false,
            Vec::new(),
            vec![grant(&recovered, wire::GrantType::ReadProtection)],
        )
        .await?;
    fixture("assert-no-reads").await?;
    println!(
        "PASS one-RPC encrypted cold reads, cached dependencies, body reuse, bounded windows, Range, roles and lost response recovery"
    );
    Ok(())
}
