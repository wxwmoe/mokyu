use hyper::{Request, Version, header::HOST};
use hyper_util::{
    rt::{TokioExecutor, TokioTimer},
    server::conn::auto::Builder,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::watch;

const IDLE: Duration = Duration::from_secs(15);

pub(crate) fn builder(streams: usize) -> Builder<TokioExecutor> {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(IDLE)
        .max_buf_size(32 * 1024);
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(streams.clamp(2, 32) as u32)
        .max_header_list_size(32 * 1024)
        .initial_stream_window_size(1024 * 1024)
        .initial_connection_window_size(2 * 1024 * 1024)
        .max_send_buf_size(256 * 1024)
        .max_pending_accept_reset_streams(32)
        .max_local_error_reset_streams(32);
    builder
}

pub(crate) struct Requests(watch::Sender<Option<usize>>);

impl Requests {
    pub(crate) fn new() -> (Self, watch::Receiver<Option<usize>>) {
        let (send, receive) = watch::channel(Some(0));
        (Self(send), receive)
    }

    pub(crate) fn start(&self, version: Version) -> Option<Active> {
        if version != Version::HTTP_2 {
            // Hyper handles subsequent HTTP/1 headers and idle keep-alive.
            if self.0.borrow().is_some() {
                self.0.send_replace(None);
            }
            return None;
        }
        self.0.send_modify(|count| *count.get_or_insert(0) += 1);
        Some(Active(self.0.clone()))
    }
}

pub(crate) struct Active(watch::Sender<Option<usize>>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.send_modify(|count| {
            if let Some(count) = count {
                *count -= 1;
            }
        });
    }
}

pub(crate) async fn idle(mut requests: watch::Receiver<Option<usize>>) {
    loop {
        let active = *requests.borrow_and_update();
        match active {
            None => std::future::pending::<()>().await,
            Some(0) => match tokio::time::timeout(IDLE, requests.changed()).await {
                Ok(Ok(())) => {}
                _ => return,
            },
            Some(_) => {
                if requests.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

pub(crate) struct ResponseBody<B> {
    body: B,
    _active: Option<Active>,
}

impl<B: hyper::body::Body> ResponseBody<B> {
    pub(crate) fn new(body: B, active: Option<Active>) -> Self {
        let active = active.filter(|_| !body.is_end_stream());
        Self {
            body,
            _active: active,
        }
    }
}

impl<B: hyper::body::Body + Unpin> hyper::body::Body for ResponseBody<B> {
    type Data = B::Data;
    type Error = B::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<B::Data>, B::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub(crate) fn authority<B>(request: &mut Request<B>) -> Result<(), s3s::S3Error> {
    if request.version() != Version::HTTP_2 {
        return Ok(());
    }
    let mut hosts = request.headers().get_all(HOST).iter();
    let host = hosts.next();
    if hosts.next().is_some() {
        return Err(s3s::s3_error!(InvalidRequest, "multiple Host fields"));
    }
    let Some(authority) = request.uri().authority() else {
        // A proxy can preserve an origin-form target and carry authority in Host.
        let host = host
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<axum::http::uri::Authority>().ok())
            .filter(|value| !value.host().is_empty() && !value.as_str().contains('@'));
        return host
            .map(|_| ())
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "HTTP authority is required"));
    };
    if let Some(host) = host {
        if !host
            .as_bytes()
            .eq_ignore_ascii_case(authority.as_str().as_bytes())
        {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "HTTP authority does not match Host"
            ));
        }
    } else {
        let host = hyper::header::HeaderValue::from_str(authority.as_str())
            .map_err(|_| s3s::s3_error!(InvalidRequest, "invalid HTTP authority"))?;
        request.headers_mut().insert(HOST, host);
    }
    Ok(())
}

pub(crate) async fn router_request(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let head = request.method() == hyper::Method::HEAD;
    let mut response = if authority(&mut request).is_err() {
        (hyper::StatusCode::BAD_REQUEST, "invalid HTTP authority").into_response()
    } else {
        next.run(request).await
    };
    if head {
        finish_head(&mut response);
    }
    response
}

pub(super) fn finish_head(response: &mut axum::response::Response) {
    // An unknown size preserves HEAD metadata without inventing Content-Length: 0.
    *response.body_mut() = axum::body::Body::from_stream(futures_util::stream::empty::<
        Result<bytes::Bytes, std::convert::Infallible>,
    >());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http2_authority_preserves_signed_paths_and_rejects_ambiguity() {
        let uri = "http://bucket.example:9000/a%2Fb+%25?x=%2F";
        let mut request = Request::builder()
            .version(Version::HTTP_2)
            .uri(uri)
            .body(())
            .unwrap();
        authority(&mut request).unwrap();
        assert_eq!(request.headers()[HOST], "bucket.example:9000");
        assert_eq!(request.uri().to_string(), uri);
        request
            .headers_mut()
            .insert(HOST, "BUCKET.example:9000".parse().unwrap());
        authority(&mut request).unwrap();
        assert_eq!(request.headers()[HOST], "BUCKET.example:9000");
        request
            .headers_mut()
            .insert(HOST, "other.example".parse().unwrap());
        assert!(authority(&mut request).is_err());
        let mut missing = Request::builder()
            .version(Version::HTTP_2)
            .uri("/")
            .body(())
            .unwrap();
        assert!(authority(&mut missing).is_err());
        missing
            .headers_mut()
            .insert(HOST, "bucket.example:9000".parse().unwrap());
        authority(&mut missing).unwrap();
        assert_eq!(missing.uri().to_string(), "/");
        missing
            .headers_mut()
            .append(HOST, "other.example".parse().unwrap());
        assert!(authority(&mut missing).is_err());
        missing.headers_mut().remove(HOST);
        missing.version_mut().clone_from(&Version::HTTP_11);
        authority(&mut missing).unwrap();
    }
}
