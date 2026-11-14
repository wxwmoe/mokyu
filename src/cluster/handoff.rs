use crate::app::App;
use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};
use tokio::sync::watch;

#[derive(Default)]
struct StateCount {
    closed: bool,
    active: usize,
}

pub(crate) struct Admission {
    state: Mutex<StateCount>,
    closed: watch::Sender<bool>,
}
impl Admission {
    pub fn new(closed: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StateCount { closed, active: 0 }),
            closed: watch::channel(closed).0,
        })
    }
    pub fn enter(self: &Arc<Self>) -> Option<Permit> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return None;
        }
        state.active += 1;
        Some(Permit(self.clone()))
    }
    fn set(&self, closed: bool) {
        let mut state = self.state.lock().unwrap();
        if state.closed != closed {
            state.closed = closed;
            self.closed.send_replace(closed);
        }
    }
    pub fn active(&self) -> usize {
        self.state.lock().unwrap().active
    }
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
}

pub(crate) struct Permit(Arc<Admission>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().active -= 1;
    }
}
struct CountedBody {
    body: Body,
    _permit: Permit,
}
impl hyper::body::Body for CountedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub async fn admit(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    let Some(permit) = app.control_admission.enter() else {
        if request.uri().path().starts_with("/mokyu.node.")
            || request
                .headers()
                .get("content-type")
                .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"))
        {
            return tonic::Status::unavailable("control is preparing to go offline")
                .into_http::<Body>();
        }
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            "Control is preparing to go offline",
        )
            .into_response();
    };
    next.run(request).await.map(|body| {
        Body::new(CountedBody {
            body,
            _permit: permit,
        })
    })
}

pub async fn refresh(app: &App) -> Result<i64> {
    let node = app.node.get().context("control identity is missing")?;
    let row = app
        .node_sessions
        .control_handoff(node, &app.secrets.credential_key)
        .await?;
    app.control_admission
        .set(row.as_ref().is_some_and(|r| r.state != "serving"));
    Ok(row.map_or(0, |r| r.revision))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closing_blocks_admission_while_existing_work_finishes() {
        let gate = Admission::new(true);
        assert!(gate.enter().is_none());
        gate.set(false);
        let active = gate.enter().unwrap();
        gate.set(true);
        assert!(gate.enter().is_none());
        assert_eq!(gate.active(), 1);
        drop(active);
        assert_eq!(gate.active(), 0);
        gate.set(false);
        assert!(gate.enter().is_some());
    }
}
