use super::{body::Reader, selection::Item};
use crate::{
    app::Metadata,
    model::journal::record::Attributes,
    storage::external::{Client, objects::Object},
    upload::Integrity,
};
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, Method, StatusCode};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) fn headers(attrs: &Attributes) -> Result<BTreeMap<String, String>> {
    let m: Metadata = serde_json::from_value(attrs.metadata.clone())?;
    let mut values = BTreeMap::from([(
        "content-type".into(),
        m.content_type
            .unwrap_or_else(|| "application/octet-stream".into()),
    )]);
    for (name, value) in [
        ("cache-control", m.cache_control),
        ("content-disposition", m.content_disposition),
        ("content-encoding", m.content_encoding),
        ("content-language", m.content_language),
        ("expires", m.expires),
    ] {
        if let Some(v) = value {
            values.insert(name.into(), v);
        }
    }
    for (name, v) in m.user.unwrap_or_default() {
        values.insert(format!("x-amz-meta-{}", name.to_ascii_lowercase()), v);
    }
    Ok(values)
}
pub(super) async fn head(
    c: &Client,
    key: &str,
    checksums: bool,
) -> Result<Option<(Object, HeaderMap)>> {
    let mut headers = HeaderMap::new();
    if checksums {
        headers.insert("x-amz-checksum-mode", "ENABLED".parse()?);
    }
    let reply = c
        .bytes(Method::HEAD, Some(key), &[], headers, Bytes::new())
        .await?;
    if reply.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let reply = c.checked(reply).await?;
    Ok(Some((
        Object::headers(reply.headers())?,
        reply.headers().clone(),
    )))
}

pub(super) async fn calculate(
    r: &Reader<'_>,
    bucket: Uuid,
    item: &Item,
    start: u64,
    end: u64,
    algo: &str,
    window: u64,
) -> Result<String> {
    let mut integrity = Integrity::new(&HeaderMap::new(), Some(algo))?;
    let mut at = start;
    while at < end {
        let data = r.read(bucket, item, at, end, window).await?;
        integrity.update(&data);
        at += data.len() as u64;
    }
    let (_, sums) = integrity.finish(None)?;
    sums.get(algo)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("unsupported checksum")
}
pub(super) async fn full(
    r: &Reader<'_>,
    bucket: Uuid,
    item: &Item,
    attrs: &Attributes,
    algo: &str,
    window: u64,
) -> Result<String> {
    if let Some(v) = attrs.checksums.get(algo).and_then(Value::as_str)
        && !v.contains('-')
    {
        return Ok(v.into());
    }
    if algo == "md5" && attrs.etag.len() == 32 {
        return Ok(STANDARD.encode(hex::decode(&attrs.etag)?));
    }
    if let Some(v) =
        r.db.query_row(
            "SELECT value FROM dump_sums WHERE bucket=? AND seq=? AND alg=?",
            params![bucket, item.seq, algo],
            |r| r.get(0),
        )
        .optional()?
    {
        return Ok(v);
    }
    let sum = calculate(r, bucket, item, 0, item.size.try_into()?, algo, window).await?;
    r.db.execute(
        "INSERT OR REPLACE INTO dump_sums VALUES(?,?,?,?)",
        params![bucket, item.seq, algo, sum],
    )?;
    Ok(sum)
}

async fn part_layout(c: &Client, key: &str, object: &Object) -> Result<Vec<u64>> {
    let mut sizes = Vec::new();
    let mut marker = 0;
    loop {
        let mut query = vec![("attributes", "")];
        if let Some(v) = &object.version {
            query.push(("versionId", v));
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-object-attributes",
            "ETag,ObjectParts,ObjectSize".parse()?,
        );
        if marker > 0 {
            headers.insert("x-amz-part-number-marker", marker.to_string().parse()?);
        }
        let reply = c
            .checked(
                c.bytes(Method::GET, Some(key), &query, headers, Bytes::new())
                    .await?,
            )
            .await?;
        let bytes = c.bounded(reply.into_body(), 2 * 1024 * 1024).await?;
        let mut xml = s3s::xml::Deserializer::new(&bytes);
        let mut tag = None;
        let mut total = None;
        let mut more = false;
        let mut next = 0;
        xml.named_element("GetObjectAttributesOutput", |xml| {
            xml.for_each_element(|xml, name| {
                match name {
                    b"ETag" => tag = Some(xml.content::<String>()?),
                    b"ObjectSize" => {
                        total = Some(
                            u64::try_from(xml.content::<i64>()?)
                                .map_err(|_| s3s::xml::DeError::UnexpectedTagName)?,
                        )
                    }
                    b"ObjectParts" => xml.for_each_element(|xml, name| {
                        match name {
                            b"IsTruncated" => more = xml.content()?,
                            b"NextPartNumberMarker" => {
                                next = usize::try_from(xml.content::<i32>()?)
                                    .map_err(|_| s3s::xml::DeError::UnexpectedTagName)?
                            }
                            b"Part" | b"Parts" => {
                                let (mut number, mut size) = (0usize, None);
                                xml.for_each_element(|xml, n| {
                                    match n {
                                        b"PartNumber" => {
                                            number = usize::try_from(xml.content::<i32>()?)
                                                .map_err(|_| s3s::xml::DeError::UnexpectedTagName)?
                                        }
                                        b"Size" => {
                                            size =
                                                Some(u64::try_from(xml.content::<i64>()?).map_err(
                                                    |_| s3s::xml::DeError::UnexpectedTagName,
                                                )?)
                                        }
                                        _ => {
                                            let _: String = xml.content()?;
                                        }
                                    }
                                    Ok(())
                                })?;
                                if number != sizes.len() + 1
                                    || size.is_none()
                                    || sizes.len() >= 10000
                                {
                                    return Err(s3s::xml::DeError::UnexpectedTagName);
                                }
                                sizes.push(size.unwrap());
                            }
                            _ => {
                                let _: String = xml.content()?;
                            }
                        }
                        Ok(())
                    })?,
                    _ => {
                        let _: String = xml.content()?;
                    }
                }
                Ok(())
            })
        })?;
        xml.expect_eof()?;
        ensure!(
            total == Some(object.size)
                && tag.is_some_and(|v| v.trim_matches('"') == object.etag.trim_matches('"')),
            "S3 object changed during verification"
        );
        if !more {
            break;
        }
        ensure!(
            next > marker && next == sizes.len(),
            "invalid S3 part cursor"
        );
        marker = next;
    }
    ensure!(
        !sizes.is_empty()
            && sizes.iter().try_fold(0u64, |a, b| a.checked_add(*b)) == Some(object.size),
        "incomplete S3 part layout"
    );
    Ok(sizes)
}

pub(super) async fn same(
    r: &Reader<'_>,
    b: Uuid,
    item: &Item,
    attrs: &Attributes,
    c: &Client,
    key: &str,
    head: &(Object, HeaderMap),
    window: u64,
) -> Result<bool> {
    let (remote, h) = head;
    if remote.size != item.size as u64 || remote.metadata != headers(attrs)? {
        return Ok(false);
    }
    let ty = h
        .get("x-amz-checksum-type")
        .map(|v| v.to_str())
        .transpose()?;
    for algo in [
        "sha256",
        "sha512",
        "crc64nvme",
        "sha1",
        "crc32c",
        "crc32",
        "md5",
    ] {
        let Some(v) = h.get(format!("x-amz-checksum-{algo}")) else {
            continue;
        };
        let value = v.to_str()?;
        if ty == Some("FULL_OBJECT") {
            return Ok(STANDARD.decode(value)?
                == STANDARD.decode(full(r, b, item, attrs, algo, window).await?)?);
        }
        if ty == Some("COMPOSITE") {
            let (value, count) = value
                .rsplit_once('-')
                .context("incomplete composite checksum")?;
            let sizes = part_layout(c, key, remote).await?;
            ensure!(
                count.parse::<usize>()? == sizes.len(),
                "composite checksum part count differs"
            );
            let mut pos = 0u64;
            let mut sum = Integrity::new(&HeaderMap::new(), Some(algo))?;
            for size in sizes {
                sum.update(
                    &STANDARD
                        .decode(calculate(r, b, item, pos, pos + size, algo, window).await?)?,
                );
                pos += size;
            }
            let (_, v) = sum.finish(None)?;
            return Ok(STANDARD.decode(value)?
                == STANDARD.decode(v[algo].as_str().context("missing composite digest")?)?);
        }
    }
    anyhow::bail!("S3 target has no comparable server checksum")
}
