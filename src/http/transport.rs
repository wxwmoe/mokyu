use hyper::{Request, Version, header::HOST};
use hyper_util::{
    rt::{TokioExecutor, TokioTimer},
    server::conn::auto::Builder,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

pub(crate) fn builder(streams: usize) -> Builder<TokioExecutor> {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(15))
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

pub(crate) async fn first_request_timeout(first: Arc<Notify>) {
    // HTTP/1 header timeouts do not cover protocol detection or an HTTP/2 preface.
    if tokio::time::timeout(Duration::from_secs(15), first.notified())
        .await
        .is_ok()
    {
        std::future::pending::<()>().await;
    }
}

pub(crate) fn authority<B>(request: &mut Request<B>) -> Result<(), s3s::S3Error> {
    if request.version() != Version::HTTP_2 {
        return Ok(());
    }
    let authority = request
        .uri()
        .authority()
        .ok_or_else(|| s3s::s3_error!(InvalidRequest, "HTTP/2 authority is required"))?;
    if let Some(host) = request.headers().get(HOST) {
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
        // An unknown size preserves HEAD metadata without inventing Content-Length: 0.
        *response.body_mut() = axum::body::Body::from_stream(futures_util::stream::empty::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >());
    }
    response
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
        missing.version_mut().clone_from(&Version::HTTP_11);
        authority(&mut missing).unwrap();
    }
}
