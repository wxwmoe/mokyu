use super::*;
use crate::{
    config::node::NodeConfig,
    node::{NodeContext, Phase},
};
use md5::{Digest, Md5};

fn materials(values: &[wire::Material], held: &mut Held) -> Result<()> {
    apply(
        &wire::ReadReply {
            materials: values.to_vec(),
            ..Default::default()
        },
        held,
    )
}

fn input(raw: &[u8], pool: &compression::Pool) -> Result<(wire::ChunkInput, Vec<u8>)> {
    let encoded = pool.compress(raw, true)?;
    let compressed = encoded.is_some();
    let encoded = encoded.unwrap_or_else(|| raw.to_vec());
    Ok((
        wire::ChunkInput {
            hash: blake3::hash(raw).as_bytes().to_vec(),
            raw_size: raw.len() as u32,
            payload_hash: blake3::hash(&encoded).as_bytes().to_vec(),
            payload_size: encoded.len() as u32,
            compressed,
        },
        encoded,
    ))
}

async fn upload(
    prepared: &wire::PreparedChunks,
    held: &Held,
    values: &[(wire::ChunkInput, Vec<u8>)],
) -> Result<Vec<wire::EncodedChunk>> {
    let keys: BTreeMap<_, _> = held
        .iter()
        .filter(|((r#type, _), _)| *r#type == wire::MaterialType::BlockKey as i32)
        .map(|(_, (_, json))| {
            let key: read::BlockKey = serde_json::from_slice(json)?;
            Ok((key.id, codec::Key::new(&key.algorithm, key.material)?))
        })
        .collect::<Result<_>>()?;
    let mut receipts = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (entry, (input, payload)) in prepared.chunks.iter().zip(values) {
        let chunk: codec::Chunk = serde_json::from_slice(&entry.chunk_json)?;
        if entry.reused || !seen.insert(chunk.uuid) {
            continue;
        }
        assert_eq!(input.payload_hash, entry.payload_hash);
        assert_eq!(
            blake3::hash(payload).as_bytes().as_slice(),
            entry.payload_hash
        );
        let storage: read::Storage = get(
            held,
            wire::MaterialType::Storage,
            &chunk.storage_id.unwrap().to_string(),
        )?;
        let store = Store::connect(
            &storage.connection,
            Options::from_profile(&storage.profile)?,
        )?;
        store.ensure_verified().await?;
        let encoded = codec::seal(&chunk, payload.clone(), &keys)?;
        let digest = blake3::hash(&encoded).as_bytes().to_vec();
        store
            .backend
            .put(
                &Path::from(crate::storage::chunk_name(chunk.location_id)),
                encoded.into(),
            )
            .await?;
        receipts.push(wire::EncodedChunk {
            chunk_id: chunk.uuid.to_string(),
            digest,
        });
    }
    Ok(receipts)
}

async fn origins(context: &NodeContext, dns: &Dns, values: Vec<Ipv4Addr>) {
    *dns.answers.write().unwrap() = Some(values.clone());
    dns.resolver.clear_cache();
    let ready = {
        let mut addresses = context.client.addresses.lock().await;
        addresses.refresh_at = Instant::now();
        addresses
            .peers
            .iter()
            .filter(|peer| values.contains(&peer.address))
            .map(|peer| peer.health.lock().unwrap().retry_at)
            .max()
            .unwrap_or_else(Instant::now)
    };
    // The next reply-loss fault needs its origin to be eligible again.
    tokio::time::sleep_until(ready).await;
    context.client.cursor.store(0, Ordering::Relaxed);
}

#[tokio::test]
#[ignore = "requires isolated controls, TLS origins and encrypted S3 writes"]
async fn node_chunk_batches_upload_once_replay_and_publish_over_rpc() -> Result<()> {
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
    let directory = path.parent().unwrap().join("chunk-runtime");
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
    let run = snapshot.lease.run.clone();
    let operation = Uuid::now_v7().to_string();
    let raw = [
        vec![17; 16 * 1024],
        vec![23; 12 * 1024],
        vec![31; 8 * 1024],
        vec![47; 4 * 1024],
    ];
    let body = [
        &raw[0], &raw[0], &raw[1], &raw[2], &raw[0], &raw[1], &raw[3],
    ]
    .into_iter()
    .flatten()
    .copied()
    .collect::<Vec<_>>();
    let preflight = context
        .client
        .prepare_request(wire::PrepareRequestInput {
            run: Some(run.clone()),
            access_key: source.access_key.clone(),
            bucket: source.bucket.clone(),
            include: vec![
                wire::MaterialType::S3Credential as i32,
                wire::MaterialType::Bucket as i32,
                wire::MaterialType::ReadPolicy as i32,
            ],
            cached: vec![],
            query: None,
        })
        .await?;
    assert_eq!(preflight.outcome, wire::ReadOutcome::Ready as i32);
    let mut authentication = Held::new();
    materials(&preflight.materials, &mut authentication)?;
    let credential: read::Credential = get(
        &authentication,
        wire::MaterialType::S3Credential,
        &source.access_key,
    )?;
    assert_eq!(credential.secret, source.secret_key);
    let begin = wire::BeginWriteRequest {
        run: Some(run.clone()),
        operation_id: operation.clone(),
        access_key: source.access_key.clone(),
        credential_revision: Some(credential.revision as u64),
        copy_read_id: None,
        part: None,
        bucket: source.bucket.clone(),
        key: "rpc-chunks".into(),
        metadata_json: br#"{"content_type":"application/octet-stream"}"#.to_vec(),
        public_read: false,
        size: Some(body.len() as u64),
        cached: vec![],
    };
    let started = Instant::now();
    let opened = context.client.begin_write(begin).await?;
    let mut resources = vec![
        context
            .resources
            .adopt(&snapshot.lease, &opened.grants, started)?,
    ];
    let mut held = Held::new();
    materials(&opened.materials, &mut held)?;
    let storage: read::Storage = get(
        &held,
        wire::MaterialType::Storage,
        &opened.storage.as_ref().unwrap().id,
    )?;
    let compression = compression::Pool::new(storage.profile.compression_config("30s"), 1)?;
    let values = raw
        .iter()
        .map(|value| input(value, &compression))
        .collect::<Result<Vec<_>>>()?;
    assert!(values.iter().all(|(value, _)| value.compressed));
    let first = vec![
        values[0].clone(),
        values[0].clone(),
        values[1].clone(),
        values[2].clone(),
    ];
    let mut request = wire::PrepareChunksRequest {
        run: Some(run.clone()),
        operation_id: operation.clone(),
        batch_id: 1,
        offset: 0,
        chunks: first.iter().map(|(value, _)| value.clone()).collect(),
        parallelism: 2,
        cached: held.values().map(|(version, _)| version.clone()).collect(),
    };
    origins(
        &context,
        &dns,
        vec![Ipv4Addr::new(127, 0, 0, 2), Ipv4Addr::new(127, 0, 0, 3)],
    )
    .await;
    fixture("chunks-lose-prepare").await?;
    let started = Instant::now();
    let prepared = context.client.prepare_chunks(request.clone()).await?;
    fixture("chunks-resume-prepare").await?;
    resources.push(
        context
            .resources
            .adopt(&snapshot.lease, &prepared.grants, started)?,
    );
    materials(&prepared.materials, &mut held)?;
    assert_eq!(prepared.chunks.len(), 4);
    assert!(!prepared.chunks.iter().any(|chunk| chunk.reused));
    assert_eq!(prepared.chunks[0].chunk_json, prepared.chunks[1].chunk_json);
    let nonces = prepared
        .chunks
        .iter()
        .map(|entry| {
            let chunk: codec::Chunk = serde_json::from_slice(&entry.chunk_json)?;
            Ok(crate::content::nonce::id(chunk.nonce.as_deref()).unwrap())
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(nonces[0], nonces[1]);
    assert_eq!(
        nonces
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    let replay = context.client.prepare_chunks(request.clone()).await?;
    assert_eq!(prepared.chunks, replay.chunks);
    let receipts = upload(&prepared, &held, &first).await?;
    assert_eq!(receipts.len(), 3);
    origins(
        &context,
        &dns,
        vec![Ipv4Addr::new(127, 0, 0, 2), Ipv4Addr::new(127, 0, 0, 3)],
    )
    .await;
    fixture("chunks-lose-commit").await?;
    let commit = wire::CommitChunksRequest {
        packed_json: None,
        run: Some(run.clone()),
        operation_id: operation.clone(),
        batch_id: 1,
        chunks: receipts,
    };
    assert_eq!(
        context.client.commit_chunks(commit.clone()).await?.end,
        prepared.end
    );
    fixture("chunks-resume-commit").await?;
    assert_eq!(
        context.client.commit_chunks(commit).await?.end,
        prepared.end
    );
    assert!(
        context
            .client
            .prepare_chunks(request.clone())
            .await?
            .committed
    );

    origins(&context, &dns, vec![Ipv4Addr::new(127, 0, 0, 2)]).await;
    request.batch_id = 2;
    request.offset = prepared.end;
    request.chunks = vec![values[0].0.clone(), values[1].0.clone()];
    request.cached = held.values().map(|(version, _)| version.clone()).collect();
    let reused = context.client.prepare_chunks(request.clone()).await?;
    assert!(reused.chunks.iter().all(|chunk| chunk.reused));
    assert!(
        !reused
            .grants
            .iter()
            .any(|grant| grant.r#type == wire::GrantType::StorageWrite as i32)
    );
    assert_eq!(
        context
            .client
            .commit_chunks(wire::CommitChunksRequest {
                packed_json: None,
                run: Some(run.clone()),
                operation_id: operation.clone(),
                batch_id: 2,
                chunks: vec![]
            })
            .await?
            .end,
        reused.end
    );
    request.batch_id = 3;
    request.offset = reused.end;
    request.chunks = vec![values[3].0.clone()];
    let started = Instant::now();
    let last = context.client.prepare_chunks(request).await?;
    resources.push(
        context
            .resources
            .adopt(&snapshot.lease, &last.grants, started)?,
    );
    materials(&last.materials, &mut held)?;
    let chunk: codec::Chunk = serde_json::from_slice(&last.chunks[0].chunk_json)?;
    assert_eq!(
        crate::content::nonce::id(chunk.nonce.as_deref()).unwrap(),
        nonces.into_iter().max().unwrap() + 1,
        "replays and reuse must not consume a nonce"
    );
    let receipts = upload(&last, &held, &values[3..]).await?;
    context
        .client
        .commit_chunks(wire::CommitChunksRequest {
            packed_json: None,
            run: Some(run.clone()),
            operation_id: operation.clone(),
            batch_id: 3,
            chunks: receipts,
        })
        .await?;
    origins(
        &context,
        &dns,
        vec![Ipv4Addr::new(127, 0, 0, 2), Ipv4Addr::new(127, 0, 0, 3)],
    )
    .await;
    fixture("chunks-lose-publish").await?;
    let publish = wire::PublishWriteRequest {
        run: Some(run.clone()),
        operation_id: operation.clone(),
        size: body.len() as u64,
        etag: format!("{:x}", Md5::digest(&body)),
        checksums_json: b"{}".to_vec(),
        if_match: None,
        if_none: Some("*".into()),
    };
    let published = context.client.publish_write(publish.clone()).await?;
    fixture("chunks-resume-publish").await?;
    assert_eq!(published.phase, wire::WritePhase::WritePublished as i32);
    assert_eq!(
        context
            .client
            .publish_write(publish.clone())
            .await?
            .published,
        published.published
    );
    let mut conflict = publish;
    conflict.if_none = None;
    assert_eq!(
        context
            .client
            .publish_write(conflict)
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let lookup = wire::WriteRequest {
        run: Some(run.clone()),
        operation_id: operation,
    };
    assert_eq!(
        context.client.abort_write(lookup.clone()).await?.phase,
        wire::WritePhase::WritePublished as i32
    );
    assert_eq!(
        context.client.write_status(lookup.clone()).await?.published,
        published.published
    );
    fixture("stop-first").await?;
    node_state(&mut states, |state| {
        state.phase == Phase::Ready
            && state.epoch > initial.epoch
            && state.peer == Some(Ipv4Addr::new(127, 0, 0, 3))
    })
    .await?;
    assert!(Arc::ptr_eq(&context.snapshot()?.lease, &snapshot.lease));
    assert_eq!(
        context.client.write_status(lookup).await?.published,
        published.published
    );
    let read = context
        .client
        .read(super::request(&run, &source, "rpc-chunks", &held))
        .await?;
    apply(&read, &mut held)?;
    assert_eq!(payload(&read, &held).await?, body);
    fixture("chunks-check-published").await?;
    drop(resources);
    context.stop();
    driver.await??;
    println!(
        "PASS encrypted S3 batch writes, duplicate chunks, three lost replies, stable nonces, pure reuse, publication receipts and API readback"
    );
    Ok(())
}
