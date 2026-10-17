use axum::http::{HeaderMap, HeaderName, HeaderValue, header};
use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use std::{
    io::{self, Write},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Encoding {
    Zstd,
    Brotli,
    Gzip,
    Identity,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Brotli => "br",
            Self::Gzip => "gzip",
            Self::Identity => "identity",
        }
    }

    pub fn etag(self, original: &str) -> String {
        if self == Self::Identity || original.starts_with("W/") {
            original.to_owned()
        } else {
            format!("W/{original}")
        }
    }
}

pub(crate) struct AcceptEncoding([Option<u16>; 5]);

impl AcceptEncoding {
    pub fn new(headers: &HeaderMap) -> Self {
        let mut weights = [None; 5];
        let mut present = false;
        for value in headers.get_all(header::ACCEPT_ENCODING) {
            let Ok(value) = value.to_str() else { continue };
            for item in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                present = true;
                let mut parts = item.split(';');
                let name = parts.next().unwrap().trim();
                let Some(index) = ["zstd", "br", "gzip", "identity", "*"]
                    .iter()
                    .position(|s| name.eq_ignore_ascii_case(s))
                else {
                    continue;
                };
                let mut weight = Some(1000);
                let mut parameter = false;
                for part in parts {
                    if parameter {
                        weight = None;
                        break;
                    }
                    parameter = true;
                    weight = part.trim().split_once('=').and_then(|(key, value)| {
                        key.trim()
                            .eq_ignore_ascii_case("q")
                            .then(|| quality(value.trim()))
                            .flatten()
                    });
                }
                let weight = weight.unwrap_or(0);
                // A duplicate exclusion wins, including a malformed explicit weight.
                weights[index] = Some(weights[index].map_or(weight, |old: u16| old.min(weight)));
            }
        }
        if !present {
            weights = [Some(0), Some(0), Some(0), Some(1000), None];
        }
        Self(weights)
    }

    pub fn identity(&self) -> bool {
        self.0[3].unwrap_or(if self.0[4] == Some(0) { 0 } else { 1000 }) > 0
    }

    pub fn select(&self, available: impl Fn(Encoding) -> bool) -> Option<Encoding> {
        let mut selected = None;
        let mut best = 0;
        for (index, encoding) in [
            Encoding::Zstd,
            Encoding::Brotli,
            Encoding::Gzip,
            Encoding::Identity,
        ]
        .into_iter()
        .enumerate()
        {
            let weight = if encoding == Encoding::Identity {
                self.0[3].unwrap_or(0)
            } else {
                self.0[index].or(self.0[4]).unwrap_or(0)
            };
            if weight > best && available(encoding) {
                selected = Some(encoding);
                best = weight;
            }
        }
        selected.or_else(|| {
            (self.identity() && available(Encoding::Identity)).then_some(Encoding::Identity)
        })
    }
}

fn quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if !matches!(whole, "0" | "1")
        || fraction.len() > 3
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if whole == "1" {
        return fraction.bytes().all(|b| b == b'0').then_some(1000);
    }
    Some(fraction.parse::<u16>().unwrap_or(0) * 10u16.pow(3 - fraction.len() as u32))
}

pub(crate) fn vary(headers: &mut HeaderMap, value: &str) {
    let mut names: Vec<String> = headers
        .get_all(header::VARY)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if names.iter().any(|name| name == "*") {
        return;
    }
    for name in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !names.iter().any(|old| old.eq_ignore_ascii_case(name)) {
            names.push(name.to_owned());
        }
    }
    if let Ok(value) = names.join(", ").parse() {
        headers.insert(header::VARY, value);
    }
}

pub(crate) fn merge(headers: &mut HeaderMap, mut extra: HeaderMap) {
    for value in extra
        .get_all(header::VARY)
        .iter()
        .filter_map(|v| v.to_str().ok())
    {
        vary(headers, value);
    }
    extra.remove(header::VARY);
    headers.extend(extra);
}

pub(crate) fn matches(headers: &HeaderMap, name: HeaderName, etag: &str, weak: bool) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|s| s.to_str().ok())
        .any(|mut value| {
            while !value.is_empty() {
                value = value.trim_start();
                if value == "*" {
                    return true;
                }
                let tag = value;
                let quoted = value.strip_prefix("W/").unwrap_or(value);
                let Some(quoted) = quoted.strip_prefix('"') else {
                    return false;
                };
                let Some(end) = quoted.find('"') else {
                    return false;
                };
                let length = value.len() - quoted.len() + end + 1;
                value = value[length..].trim_start();
                if !value.is_empty() && !value.starts_with(',') {
                    return false;
                }
                let tag = &tag[..length];
                if if weak {
                    tag.trim_start_matches("W/") == etag.trim_start_matches("W/")
                } else {
                    !tag.starts_with("W/") && !etag.starts_with("W/") && tag == etag
                } {
                    return true;
                }
                value = value.strip_prefix(',').unwrap_or(value);
            }
            false
        })
}

pub(crate) fn no_transform(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("no-transform"))
}

pub(crate) fn encoded_headers(headers: &mut HeaderMap, encoding: Encoding) {
    vary(headers, "Accept-Encoding");
    if encoding != Encoding::Identity {
        headers.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static(encoding.name()),
        );
        headers.remove(header::CONTENT_LENGTH);
        headers.remove(header::ACCEPT_RANGES);
        if let Some(tag) = headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| encoding.etag(v).parse().ok())
        {
            headers.insert(header::ETAG, tag);
        }
    }
}

enum Encoder {
    Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
    Brotli(Box<brotli::enc::encode::BrotliEncoderStateStruct<brotli::enc::StandardAlloc>>),
    Gzip(flate2::write::GzEncoder<Vec<u8>>),
}

impl Drop for Encoder {
    fn drop(&mut self) {
        if let Self::Brotli(encoder) = self {
            brotli::enc::encode::BrotliEncoderDestroyInstance(encoder);
        }
    }
}

impl Encoder {
    fn new(encoding: Encoding) -> io::Result<Self> {
        Ok(match encoding {
            Encoding::Zstd => {
                let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3)?;
                encoder.window_log(20)?;
                Self::Zstd(encoder)
            }
            Encoding::Brotli => {
                use brotli::enc::encode::{BrotliEncoderParameter, BrotliEncoderStateStruct};
                let mut encoder = Box::new(BrotliEncoderStateStruct::new(
                    brotli::enc::StandardAlloc::default(),
                ));
                encoder.set_parameter(BrotliEncoderParameter::BROTLI_PARAM_QUALITY, 4);
                encoder.set_parameter(BrotliEncoderParameter::BROTLI_PARAM_LGWIN, 20);
                Self::Brotli(encoder)
            }
            Encoding::Gzip => Self::Gzip(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::new(6),
            )),
            Encoding::Identity => return Err(io::Error::other("identity has no encoder")),
        })
    }

    fn step(&mut self, data: &[u8], finish: bool) -> io::Result<Vec<u8>> {
        match self {
            Self::Zstd(encoder) => {
                encoder.write_all(data)?;
                if finish {
                    encoder.do_finish()?;
                } else {
                    encoder.flush()?;
                }
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Gzip(encoder) => {
                encoder.write_all(data)?;
                if finish {
                    encoder.try_finish()?;
                } else {
                    encoder.flush()?;
                }
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Brotli(encoder) => {
                use brotli::enc::encode::BrotliEncoderOperation;
                let operation = if finish {
                    BrotliEncoderOperation::BROTLI_OPERATION_FINISH
                } else {
                    BrotliEncoderOperation::BROTLI_OPERATION_FLUSH
                };
                let mut output = Vec::new();
                let mut remaining = data.len();
                let mut offset = 0;
                let mut total = None;
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let mut available = buffer.len();
                    let mut written = 0;
                    if !encoder.compress_stream(
                        operation,
                        &mut remaining,
                        data,
                        &mut offset,
                        &mut available,
                        &mut buffer,
                        &mut written,
                        &mut total,
                        &mut |_, _, _, _| (),
                    ) {
                        return Err(io::Error::other("Brotli encoding failed"));
                    }
                    output.extend_from_slice(&buffer[..written]);
                    if remaining == 0
                        && !encoder.has_more_output()
                        && (!finish || encoder.is_finished())
                    {
                        break;
                    }
                }
                Ok(output)
            }
        }
    }
}

struct Output {
    bytes: Vec<u8>,
    _memory: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Output {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;
pub(crate) type Complete = Box<dyn FnOnce(u64, u64) + Send>;

pub(crate) fn encode(
    mut source: ByteStream,
    encoding: Encoding,
    expected: Option<u64>,
    cpu: Arc<Semaphore>,
    memory: OwnedSemaphorePermit,
    complete: Option<Complete>,
) -> ByteStream {
    Box::pin(async_stream::try_stream! {
        let memory = Arc::new(memory);
        let permit = cpu.clone().acquire_owned().await.map_err(io::Error::other)?;
        let held = memory.clone();
        let mut encoder = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            Encoder::new(encoding).map(|codec| (codec, held))
        }).await.map_err(io::Error::other)??;
        let mut raw_size = 0u64;
        let mut encoded_size = 0u64;
        while let Some(data) = source.next().await {
            let data = data?;
            raw_size = raw_size.checked_add(data.len() as u64).ok_or_else(|| io::Error::other("response size overflow"))?;
            if expected.is_some_and(|length| raw_size > length) { Err(io::Error::other("response exceeded expected size"))?; }
            for start in (0..data.len()).step_by(64 * 1024) {
                // Copy a bounded slice: the blocking task must not retain an uncharged source allocation after cancellation.
                let input = data.slice(start..(start + 64 * 1024).min(data.len())).to_vec();
                let permit = cpu.clone().acquire_owned().await.map_err(io::Error::other)?;
                let held = memory.clone();
                let (next, output) = tokio::task::spawn_blocking(move || {
                    let (_permit, _memory) = (permit, held);
                    let output = encoder.0.step(&input, false)?;
                    Ok::<_, io::Error>((encoder, output))
                }).await.map_err(io::Error::other)??;
                encoder = next;
                encoded_size = encoded_size.checked_add(output.len() as u64).ok_or_else(|| io::Error::other("encoded size overflow"))?;
                if !output.is_empty() { yield Bytes::from_owner(Output { bytes: output, _memory: memory.clone() }); }
            }
        }
        if expected.is_some_and(|length| raw_size != length) { Err(io::Error::other("response ended before expected size"))?; }
        #[cfg(feature="fault-injection")]
        crate::faults::point("http-compression-finish").await;
        let permit = cpu.acquire_owned().await.map_err(io::Error::other)?;
        let held = memory.clone();
        let output = tokio::task::spawn_blocking(move || {
            let (_permit, _memory) = (permit, held);
            encoder.0.step(&[], true)
        }).await.map_err(io::Error::other)??;
        encoded_size = encoded_size.checked_add(output.len() as u64).ok_or_else(|| io::Error::other("encoded size overflow"))?;
        if let Some(complete) = complete { complete(raw_size, encoded_size); }
        if !output.is_empty() { yield Bytes::from_owner(Output { bytes: output, _memory: memory }); }
    })
}

pub(crate) async fn json(
    State(app): State<Arc<crate::app::App>>,
    request: Request,
    next: Next,
) -> Response {
    use axum::http::{Method, StatusCode};
    // The schema contains no credentials or reflected input. Other API routes retain their own security policy.
    let eligible = request.uri().path() == "/api/v1/openapi.json"
        && matches!(*request.method(), Method::GET | Method::HEAD);
    let head = request.method() == Method::HEAD;
    let transform = !no_transform(request.headers());
    let accept = AcceptEncoding::new(request.headers());
    let mut response = next.run(request).await;
    if !eligible || response.status() != StatusCode::OK {
        return response;
    }
    let length = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let compress = transform
        && !no_transform(response.headers())
        && !response.headers().contains_key(header::CONTENT_ENCODING)
        && length.is_none_or(|length| length >= 1024);
    let Some(mut encoding) = accept.select(|encoding| encoding == Encoding::Identity || compress)
    else {
        let mut response = StatusCode::NOT_ACCEPTABLE.into_response();
        vary(response.headers_mut(), "Accept-Encoding");
        return response;
    };
    let memory = (encoding != Encoding::Identity && !head)
        .then(|| app.storage.http_work())
        .flatten();
    if encoding != Encoding::Identity && !head && memory.is_none() {
        if !accept.identity() {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        encoding = Encoding::Identity;
    }
    encoded_headers(response.headers_mut(), encoding);
    if encoding != Encoding::Identity && head {
        // An exact size hint would let Hyper restore the uncompressed Content-Length on HEAD.
        *response.body_mut() =
            Body::from_stream(futures_util::stream::empty::<io::Result<Bytes>>());
    }
    if encoding != Encoding::Identity && !head {
        let source = std::mem::replace(response.body_mut(), Body::empty())
            .into_data_stream()
            .map(|v| v.map_err(io::Error::other));
        *response.body_mut() = Body::from_stream(encode(
            Box::pin(source),
            encoding,
            length,
            app.storage.cpu.clone(),
            memory.unwrap(),
            None,
        ));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_respects_weights_and_exclusions() {
        for (input, expected) in [
            ("", Some(Encoding::Identity)),
            ("gzip, deflate, br, zstd", Some(Encoding::Zstd)),
            ("ZSTD;q=0.2, br;q=0.8", Some(Encoding::Brotli)),
            ("gzip;q=0.1", Some(Encoding::Gzip)),
            ("gzip;q=0.1, identity;q=0.2", Some(Encoding::Identity)),
            ("*;q=0.5, zstd;q=0", Some(Encoding::Brotli)),
            ("*;q=0", None),
            ("*;q=0, identity;q=0.1", Some(Encoding::Identity)),
            (
                "gzip;q=1.001, zstd;q=garbage, br;q=.5",
                Some(Encoding::Identity),
            ),
            ("gzip;q=0, gzip;q=1, identity;q=0", None),
            ("zstd;q=1;q=0, gzip;q=0.001", Some(Encoding::Gzip)),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT_ENCODING, input.parse().unwrap());
            assert_eq!(
                AcceptEncoding::new(&headers).select(|_| true),
                expected,
                "{input}"
            );
        }
        let mut headers = HeaderMap::new();
        assert_eq!(
            AcceptEncoding::new(&headers).select(|_| true),
            Some(Encoding::Identity)
        );
        headers.append(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        headers.append(header::ACCEPT_ENCODING, "zstd".parse().unwrap());
        assert_eq!(
            AcceptEncoding::new(&headers).select(|e| e != Encoding::Zstd),
            Some(Encoding::Gzip)
        );
    }

    #[test]
    fn validators_and_vary_preserve_other_variants() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            "W/\"one,two\", \"three\"".parse().unwrap(),
        );
        assert!(matches(
            &headers,
            header::IF_NONE_MATCH,
            "\"one,two\"",
            true
        ));
        assert!(!matches(
            &headers,
            header::IF_NONE_MATCH,
            "\"one,two\"",
            false
        ));
        assert!(matches(&headers, header::IF_NONE_MATCH, "\"three\"", false));
        headers.append(header::VARY, "Origin, Cookie".parse().unwrap());
        headers.append(header::VARY, "Authorization".parse().unwrap());
        vary(&mut headers, "Accept-Encoding, origin");
        assert_eq!(
            headers[header::VARY],
            "Origin, Cookie, Authorization, Accept-Encoding"
        );
        vary(&mut headers, "*");
        vary(&mut headers, "Test");
        assert!(!headers[header::VARY].to_str().unwrap().contains("Test"));
    }

    #[tokio::test]
    async fn streaming_codecs_release_cpu_between_source_reads() {
        use std::io::Read;
        for encoding in [Encoding::Zstd, Encoding::Brotli, Encoding::Gzip] {
            let original = "streaming response payload ".repeat(7000).into_bytes();
            let cpu = Arc::new(Semaphore::new(1));
            let memory = Arc::new(Semaphore::new(1));
            let input = original.clone();
            let source_cpu = cpu.clone();
            let source = async_stream::try_stream! {
                for chunk in input.chunks(37000) {
                    let permit = source_cpu.acquire().await.map_err(io::Error::other)?;
                    let bytes = Bytes::copy_from_slice(chunk);
                    drop(permit);
                    yield bytes;
                }
            };
            let observed = Arc::new(std::sync::Mutex::new(None));
            let completed = observed.clone();
            let mut stream = encode(
                Box::pin(source),
                encoding,
                Some(original.len() as u64),
                cpu.clone(),
                memory.clone().acquire_owned().await.unwrap(),
                Some(Box::new(move |raw, encoded| {
                    *completed.lock().unwrap() = Some((raw, encoded));
                })),
            );
            let encoded = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let mut bytes = Vec::new();
                while let Some(part) = stream.next().await {
                    bytes.extend_from_slice(&part.unwrap());
                }
                bytes
            })
            .await
            .expect("single CPU permit must make progress");
            assert_eq!(
                *observed.lock().unwrap(),
                Some((original.len() as u64, encoded.len() as u64))
            );
            let decoded = match encoding {
                Encoding::Zstd => zstd::decode_all(encoded.as_slice()).unwrap(),
                Encoding::Brotli => {
                    let mut bytes = Vec::new();
                    brotli::Decompressor::new(encoded.as_slice(), 4096)
                        .read_to_end(&mut bytes)
                        .unwrap();
                    bytes
                }
                Encoding::Gzip => {
                    let mut bytes = Vec::new();
                    flate2::read::GzDecoder::new(encoded.as_slice())
                        .read_to_end(&mut bytes)
                        .unwrap();
                    bytes
                }
                Encoding::Identity => unreachable!(),
            };
            assert_eq!(decoded, original);
            assert_eq!(cpu.available_permits(), 1);
            assert_eq!(memory.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn incomplete_sources_never_finish_the_encoding() {
        for encoding in [Encoding::Zstd, Encoding::Brotli, Encoding::Gzip] {
            let cpu = Arc::new(Semaphore::new(1));
            let memory = Arc::new(Semaphore::new(1));
            let source = futures_util::stream::iter([Ok(Bytes::from_static(b"short"))]);
            let mut stream = encode(
                Box::pin(source),
                encoding,
                Some(100),
                cpu,
                memory.clone().acquire_owned().await.unwrap(),
                Some(Box::new(|_, _| {
                    panic!("incomplete response must not be learned")
                })),
            );
            let mut failed = false;
            while let Some(part) = stream.next().await {
                if part.is_err() {
                    failed = true;
                }
            }
            assert!(failed);
            drop(stream);
            assert_eq!(memory.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn canceled_output_keeps_its_memory_charge() {
        let cpu = Arc::new(Semaphore::new(1));
        let memory = Arc::new(Semaphore::new(1));
        let source = futures_util::stream::iter([Ok(Bytes::from_static(b"retained frame"))])
            .chain(futures_util::stream::pending());
        let mut stream = encode(
            Box::pin(source),
            Encoding::Gzip,
            None,
            cpu.clone(),
            memory.clone().acquire_owned().await.unwrap(),
            Some(Box::new(|_, _| {
                panic!("canceled response must not be learned")
            })),
        );
        let output = stream.next().await.unwrap().unwrap();
        drop(stream);
        assert_eq!(cpu.available_permits(), 1);
        assert_eq!(memory.available_permits(), 0);
        drop(output);
        assert_eq!(memory.available_permits(), 1);
    }
}
