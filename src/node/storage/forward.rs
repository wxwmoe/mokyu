use super::{
    Node, fragments,
    transfer::{self, Authorization},
};
use crate::{
    content::fragment::{Header, UNIT},
    model::{
        storage_payload::Receipt,
        storage_transfer::{Action, Destination, GRANT_LIMIT, SignedGrant, SignedReceipt},
    },
    rpc::wire,
    storage::native::transport::{Pool, Renewal},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::StreamExt;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub(super) fn plan(node: &Node, bytes: &[u8]) -> Result<Vec<Destination>> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= GRANT_LIMIT,
        "invalid forwarding plan size"
    );
    let targets: Vec<Destination> = serde_json::from_slice(bytes)?;
    ensure!(targets.len() == 2, "forwarding requires two destinations");
    let manifest = node
        .files
        .manifest
        .read()
        .unwrap()
        .clone()
        .context("storage membership is missing")?;
    let mut region = None;
    let mut original: Option<Header> = None;
    for (index, target) in targets.iter().enumerate() {
        target.authorization.verify(&manifest)?;
        let grant = &target.authorization.grant;
        let Action::Write { header } = &grant.action else {
            anyhow::bail!("forwarding requires write grants");
        };
        let member = manifest
            .members
            .iter()
            .find(|member| member.media_id == grant.media)
            .context("forwarding member is missing")?;
        ensure!(
            member.state == "available" && target.url == member.topology.url,
            "forwarding destination differs from its membership"
        );
        if index == 0 {
            ensure!(
                grant.node == node.client.identity.node_id
                    && grant.media == node.files.media.media_id,
                "forwarding starts at another storage"
            );
            ensure!(
                header.layout.data_shards == 1 && header.layout.parity_shards == 0,
                "forwarding requires identical replica bytes"
            );
            region = Some(
                member
                    .topology
                    .region
                    .as_ref()
                    .context("forwarding requires a region")?,
            );
            original = Some(header.clone());
        } else {
            ensure!(
                member.topology.region.as_ref() == region,
                "forwarding cannot cross regions"
            );
            let first = original.as_ref().unwrap();
            ensure!(
                header.media != first.media
                    && header.fragment != first.fragment
                    && header.copy != first.copy,
                "forwarding requires different replica destinations"
            );
            let mut expected = first.clone();
            expected.media = header.media;
            expected.fragment = header.fragment;
            expected.copy = header.copy;
            ensure!(
                expected == *header,
                "forwarding changed the payload or layout"
            );
        }
    }
    Ok(targets)
}

struct Grants {
    auth: Arc<Authorization>,
    peer: Mutex<Destination>,
}

#[async_trait::async_trait]
impl Renewal for Grants {
    async fn renew(&self, grant: &SignedGrant) -> Result<SignedGrant> {
        self.auth.check()?;
        let next = self.peer.lock().unwrap().authorization.clone();
        grant.grant.renewal(&next.grant)?;
        ensure!(
            next.grant.expires > chrono::Utc::now().timestamp(),
            "forwarding grant expired"
        );
        Ok(next)
    }
}

pub(super) async fn write(
    node: &Node,
    pool: &Pool,
    mut input: tonic::Streaming<wire::StorageInput>,
    auth: Arc<Authorization>,
    targets: Vec<Destination>,
) -> Result<(Receipt, SignedReceipt)> {
    let Action::Write { header } = auth.value().action else {
        anyhow::bail!("forwarding requires a write grant");
    };
    let grants = Grants {
        auth: auth.clone(),
        peer: Mutex::new(targets[1].clone()),
    };
    let (local, receiver) = mpsc::channel(2);
    let (peer, remote) = mpsc::channel::<Bytes>(2);
    let feed_auth = auth.clone();
    let feed_grants = &grants;
    let feeding = async move {
        loop {
            let message = transfer::next(&mut input).await?;
            feed_auth.check()?;
            if !message.grant_json.is_empty() {
                ensure!(
                    message.data.is_empty() && !message.complete,
                    "renewal must not contain fragment bytes"
                );
                let next = plan(node, &message.grant_json)?;
                feed_auth.renew(&serde_json::to_vec(&next[0].authorization)?)?;
                let mut current = feed_grants.peer.lock().unwrap();
                ensure!(
                    current.url == next[1].url,
                    "forwarding renewal changed its destination"
                );
                current
                    .authorization
                    .grant
                    .renewal(&next[1].authorization.grant)?;
                *current = next[1].clone();
            } else if message.complete {
                ensure!(
                    message.data.is_empty(),
                    "completion must follow the last fragment byte"
                );
                local
                    .send(fragments::Input::Complete)
                    .await
                    .context("local writer stopped")?;
                return Ok::<_, anyhow::Error>(());
            } else {
                ensure!(
                    !message.data.is_empty() && message.data.len() <= UNIT,
                    "forwarding data exceeds its bound"
                );
                let data = Bytes::from(message.data);
                local
                    .send(fragments::Input::Data(data.clone()))
                    .await
                    .context("local writer stopped")?;
                peer.send(data).await.context("forwarding writer stopped")?;
            }
        }
    };
    let body = futures_util::stream::unfold(remote, |mut receiver| async move {
        receiver.recv().await.map(|bytes| (Ok(bytes), receiver))
    })
    .boxed();
    let writing = async {
        let (local, remote) = tokio::try_join!(
            fragments::write(&node.files, header, receiver, auth.checking()),
            pool.write(targets[1].clone(), body, &grants),
        )?;
        Ok::<_, anyhow::Error>((local, remote.receipt))
    };
    tokio::pin!(feeding, writing);
    tokio::select! {
        result = &mut writing => {
            let result = result?;
            feeding.await?;
            Ok(result)
        }
        fed = &mut feeding => {
            let result = writing.await?;
            fed?;
            Ok(result)
        }
    }
}
