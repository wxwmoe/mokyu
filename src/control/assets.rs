pub(super) const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'sha256-60LHlRjW/B3CtzIoE/Lf1/NEDvko9efWMFaGVhHu/cs='; style-src-attr 'unsafe-inline'; img-src 'self' blob: data:; font-src 'self'; media-src 'self' blob:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";
use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[cfg(feature = "web-ui")]
pub(super) struct Asset {
    path: &'static str,
    mime: &'static str,
    etag: &'static str,
    immutable: bool,
    data: &'static [u8],
    variants: [Option<&'static [u8]>; 3],
}
#[cfg(feature = "web-ui")]
include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));

pub(super) async fn serve(request: Request) -> Response {
    #[cfg(not(feature = "web-ui"))]
    {
        let _ = request;
        StatusCode::NOT_FOUND.into_response()
    }
    #[cfg(feature = "web-ui")]
    {
        use crate::http::compression::{self, AcceptEncoding, Encoding};
        use axum::http::{Method, header};
        if request.method() != Method::GET && request.method() != Method::HEAD {
            return (
                StatusCode::METHOD_NOT_ALLOWED,
                [(header::ALLOW, "GET, HEAD")],
            )
                .into_response();
        }
        let path = request.uri().path();
        if path == "/api" || path.starts_with("/api/") {
            return StatusCode::NOT_FOUND.into_response();
        }
        let path = match path {
            "/" => "/index.html",
            "/favicon.ico" => "/assets/favicon.ico",
            value => value,
        };
        let asset = ASSETS.iter().find(|a| a.path == path).or_else(|| {
            let html = request
                .headers()
                .get(header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains("text/html"));
            (html && !path.rsplit('/').next().unwrap_or("").contains('.'))
                .then(|| ASSETS.iter().find(|a| a.path == "/index.html"))
                .flatten()
        });
        let Some(asset) = asset else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let accept = AcceptEncoding::new(request.headers());
        let identity = accept.identity() && request.headers().contains_key(header::IF_MATCH);
        let transform = !compression::no_transform(request.headers()) && !identity;
        let variant = |encoding| match encoding {
            Encoding::Zstd => asset.variants[0],
            Encoding::Brotli => asset.variants[1],
            Encoding::Gzip => asset.variants[2],
            Encoding::Identity => Some(asset.data),
        };
        let Some(encoding) = accept.select(|encoding| {
            (encoding == Encoding::Identity || transform) && variant(encoding).is_some()
        }) else {
            let mut response = StatusCode::NOT_ACCEPTABLE.into_response();
            compression::vary(response.headers_mut(), "Accept-Encoding");
            return response;
        };
        let data = variant(encoding).unwrap();
        let etag = encoding.etag(asset.etag);
        let failed = request.headers().contains_key(header::IF_MATCH)
            && !compression::matches(request.headers(), header::IF_MATCH, &etag, false);
        let unchanged = compression::matches(request.headers(), header::IF_NONE_MATCH, &etag, true);
        let mut response = if failed {
            StatusCode::PRECONDITION_FAILED.into_response()
        } else if unchanged {
            StatusCode::NOT_MODIFIED.into_response()
        } else {
            data.into_response()
        };
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, asset.mime.parse().unwrap());
        headers.insert(header::ETAG, asset.etag.parse().unwrap());
        compression::encoded_headers(headers, encoding);
        if failed || unchanged {
            headers.remove(header::CONTENT_ENCODING);
        }
        if !failed && !unchanged {
            headers.insert(
                header::CONTENT_LENGTH,
                data.len().to_string().parse().unwrap(),
            );
        }
        headers.insert(
            header::CACHE_CONTROL,
            if asset.immutable {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            }
            .parse()
            .unwrap(),
        );
        // Reka SelectViewport injects this fixed scrollbar stylesheet. Keep script execution external-only.
        headers.insert(header::CONTENT_SECURITY_POLICY, CSP.parse().unwrap());
        if request.method() == Method::HEAD {
            *response.body_mut() = axum::body::Body::empty();
        }
        response
    }
}

#[cfg(all(test, not(feature = "web-ui")))]
mod tests {
    #[tokio::test]
    async fn api_only_has_no_embedded_frontend() {
        for path in ["/", "/media", "/assets/mokyu-icon.svg"] {
            let request = axum::http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(
                super::serve(request).await.status(),
                axum::http::StatusCode::NOT_FOUND
            );
        }
    }
}
