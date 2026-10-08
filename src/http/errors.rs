use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub(crate) struct HttpError(pub(crate) anyhow::Error);
#[derive(Clone)]
pub(crate) struct ErrorCode(pub String);
impl<E: Into<anyhow::Error>> From<E> for HttpError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let Self(error) = self;
        let error = crate::app::quota::database_error(&error)
            .map(anyhow::Error::from)
            .unwrap_or(error);
        let this = Self(error);
        this.response()
    }
}
impl HttpError {
    fn response(self) -> Response {
        let status = if let Some(e) = self.0.downcast_ref::<s3s::S3Error>() {
            e.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        if status.is_server_error() {
            tracing::error!(error=%self.0,"HTTP request failed");
        }
        if status == StatusCode::NOT_MODIFIED {
            return status.into_response();
        }
        let mut response = (
            status,
            Json(json!({"error":status.canonical_reason().unwrap_or("request failed")})),
        )
            .into_response();
        response.extensions_mut().insert(ErrorCode(
            self.0
                .downcast_ref::<s3s::S3Error>()
                .map(|error| error.code().as_str().to_owned())
                .unwrap_or_else(|| "InternalError".into()),
        ));
        if let Some(headers) = self
            .0
            .downcast_ref::<s3s::S3Error>()
            .and_then(|e| e.headers())
        {
            response.headers_mut().extend(headers.clone());
        }
        response
    }
}
pub(crate) fn unauthorized() -> HttpError {
    s3s::s3_error!(AccessDenied).into()
}
pub(crate) fn problem(status: StatusCode, code: &'static str) -> HttpError {
    let mut error = s3s::S3Error::new(s3s::S3ErrorCode::Custom(code.into()));
    error.set_status_code(status);
    error.into()
}
impl HttpError {
    pub(super) fn has_code(&self, code: s3s::S3ErrorCode) -> bool {
        self.0
            .downcast_ref::<s3s::S3Error>()
            .is_some_and(|e| e.code() == &code)
    }
}
