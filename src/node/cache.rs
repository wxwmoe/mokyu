use super::reads::{Prepared, Reads};
use crate::{
    app::responses::{Builder, Entry, Key},
    http::{
        compression::{AcceptEncoding, Encoding},
        object,
    },
    model::node_read as read,
    rpc::wire,
};
use anyhow::{Context, Result};
use axum::http::{HeaderMap, HeaderName, HeaderValue, header};
use prost::Message;
use std::sync::Arc;

pub(super) fn route(input: &wire::ReadRequest) -> [u8; 32] {
    let bytes = input
        .route
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    *blake3::hash(&bytes).as_bytes()
}

pub(super) fn claim(entry: &Entry) -> Option<wire::CachedBody> {
    let proof = entry.node_body()?;
    Some(wire::CachedBody {
        object: Some(crate::rpc::read::version(proof.object)),
        representation: proof.representation,
        policy: proof.policy,
        mapping: Some(crate::rpc::read::version(proof.mapping)),
        storages: proof
            .storages
            .into_iter()
            .map(crate::rpc::read::version)
            .collect(),
        verified: proof.verified,
    })
}

impl Reads {
    pub(super) fn candidate(&self, input: &wire::ReadRequest) -> Option<(Key, Arc<Entry>)> {
        if input.method != wire::ReadMethod::Get as i32
            || input
                .include
                .contains(&(wire::ReadInclude::Protection as i32))
        {
            return None;
        }
        let route = route(input);
        let mut headers = HeaderMap::new();
        for field in &input.headers {
            headers.append(
                HeaderName::from_bytes(field.name.as_bytes()).ok()?,
                HeaderValue::from_bytes(&field.value).ok()?,
            );
        }
        let encoding = if input.access_key.is_empty() && !headers.contains_key(header::RANGE) {
            AcceptEncoding::new(&headers).select(|_| true)?
        } else {
            Encoding::Identity
        };
        self.bodies
            .node_candidate(route, encoding.name())
            .or_else(|| self.bodies.node_candidate(route, "identity"))
    }

    pub(super) fn cache_body(
        &self,
        prepared: &Prepared,
        object: &read::Object,
        encoding: Encoding,
        headers: &HeaderMap,
    ) -> Result<Option<Builder>> {
        if prepared.reply.http_status != 200
            || !object::cacheable(headers)
            || prepared.reply.offset != 0
            || prepared.reply.length != object.size as u64
            || prepared.reply.next.is_some()
            || !self.bodies.eligible(&object.stream())
        {
            return Ok(None);
        }
        let mut proof = if let Some(entry) = &prepared.cached_body {
            entry.node_body().context("cached body proof missing")?
        } else {
            let Some(mapping) = &prepared.reply.mapping else {
                return Ok(None);
            };
            let value: read::Mapping = prepared.material(mapping)?;
            if value.start != 0 || value.end != object.size || value.segments.len() > 64 {
                return Ok(None);
            }
            let mut storages = prepared.storage_versions();
            storages.sort_by(|left, right| left.id.cmp(&right.id));
            read::CachedBody {
                object: crate::rpc::read::cached(vec![
                    prepared
                        .reply
                        .object
                        .clone()
                        .context("object version missing")?,
                ])?
                .remove(0),
                representation: encoding.name().into(),
                policy: object::POLICY as u32,
                mapping: crate::rpc::read::cached(vec![mapping.clone()])?.remove(0),
                storages: crate::rpc::read::cached(storages)?,
                verified: false,
            }
        };
        proof.representation = encoding.name().into();
        proof.verified = false;
        let key = Key {
            bucket: object.bucket,
            stream: object.id.into(),
            encoding: encoding.name(),
            policy: object::POLICY,
        };
        let mut builder = self
            .bodies
            .begin_node(key, &object.stream(), prepared.route, proof);
        if let (Some(builder), Some(entry)) = (&mut builder, &prepared.cached_body) {
            builder.reuse(entry);
        }
        Ok(builder)
    }
}
