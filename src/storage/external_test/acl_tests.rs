use super::*;
use axum::{Router, extract::State as Shared, http::Request, routing::any};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};
use uuid::Uuid;

#[derive(Clone)]
struct Endpoint {
    id: Uuid,
    mode: &'static str,
    public: Arc<AtomicBool>,
    writes: Arc<AtomicUsize>,
    public_attempts: Arc<AtomicUsize>,
}

async fn reply(Shared(state): Shared<Endpoint>, request: Request<Body>) -> Response<Body> {
    assert_eq!(
        request.uri().path(),
        format!("/bucket/.mokyu-tests/{}/write", state.id)
    );
    assert!(
        request
            .uri()
            .query()
            .unwrap()
            .contains("versionId=original%2Fversion")
    );
    let response = |status, body: String| {
        Response::builder()
            .status(status)
            .body(Body::from(body))
            .unwrap()
    };
    if !request.headers().contains_key(header::AUTHORIZATION) {
        assert!(!request.headers().contains_key("x-amz-date"));
        assert!(!request.headers().contains_key("x-amz-content-sha256"));
        assert_eq!(request.method(), Method::GET);
        return if (state.public.load(Relaxed) && state.mode != "ignore-public")
            || state.mode == "public-policy"
        {
            Response::new(Body::from(state.id.as_bytes().repeat(1024)))
        } else {
            response(403, String::new())
        };
    }
    if request.uri().query().unwrap().starts_with("acl=") {
        if request.method() == Method::GET {
            if state.mode == "read-denied" {
                return response(403, String::new());
            }
            if state.mode == "malformed" {
                return response(200, "<AccessControlPolicy/>".into());
            }
            let mut grants = r#"<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>"#.to_owned();
            if state.public.load(Relaxed) {
                grants.push_str(r#"<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>"#);
            }
            return response(
                200,
                format!(
                    "<AccessControlPolicy><AccessControlList>{grants}</AccessControlList></AccessControlPolicy>"
                ),
            );
        }
        assert_eq!(request.method(), Method::PUT);
        state.writes.fetch_add(1, Relaxed);
        if state.mode == "disabled" {
            return response(
                400,
                "<Error><Code>AccessControlListNotSupported</Code></Error>".into(),
            );
        }
        if state.mode == "write-denied" {
            return response(403, String::new());
        }
        if state.mode == "embedded-error" {
            return response(200, "<Error><Code>AccessDenied</Code></Error>".into());
        }
        let public = request.headers()["x-amz-acl"] == "public-read";
        if public && state.mode == "block-public" {
            return response(403, String::new());
        }
        if public || state.mode != "ignore-private" {
            state.public.store(public, Relaxed);
        }
        if public && state.mode == "uncertain" && state.public_attempts.fetch_add(1, Relaxed) == 0 {
            return response(503, String::new());
        }
        return response(200, String::new());
    }
    assert_eq!(request.method(), Method::GET);
    Response::builder()
        .header(
            "x-amz-meta-mokyu-test",
            if state.mode == "foreign" {
                Uuid::nil().to_string()
            } else {
                state.id.to_string()
            },
        )
        .header(header::ETAG, "\"content\"")
        .body(Body::from(state.id.as_bytes().repeat(1024)))
        .unwrap()
}

#[tokio::test]
async fn external_acls_verify_effective_access_and_recover_uncertain_writes() -> Result<()> {
    for (mode, expected) in [
        (
            "normal",
            [State::Passed, State::Passed, State::Passed, State::Passed],
        ),
        (
            "read-denied",
            [State::Denied, State::Passed, State::Passed, State::Passed],
        ),
        (
            "disabled",
            [
                State::Passed,
                State::Unsupported,
                State::Skipped,
                State::Skipped,
            ],
        ),
        (
            "write-denied",
            [State::Passed, State::Denied, State::Skipped, State::Skipped],
        ),
        (
            "embedded-error",
            [State::Passed, State::Failed, State::Skipped, State::Skipped],
        ),
        (
            "block-public",
            [State::Passed, State::Passed, State::Blocked, State::Passed],
        ),
        (
            "ignore-public",
            [State::Passed, State::Passed, State::Blocked, State::Passed],
        ),
        (
            "public-policy",
            [State::Passed, State::Passed, State::Passed, State::Blocked],
        ),
        (
            "ignore-private",
            [State::Passed, State::Passed, State::Passed, State::Blocked],
        ),
        (
            "uncertain",
            [State::Passed, State::Passed, State::Passed, State::Passed],
        ),
        (
            "malformed",
            [State::Failed, State::Passed, State::Passed, State::Passed],
        ),
        ("foreign", [State::Skipped; 4]),
    ] {
        let id = Uuid::now_v7();
        let state = Endpoint {
            id,
            mode,
            public: Arc::new(AtomicBool::new(false)),
            writes: Arc::new(AtomicUsize::new(0)),
            public_attempts: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = Router::new().fallback(any(reply)).with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let connection = super::super::external::Connection {
            endpoint_url: format!("http://{address}"),
            region: "test".into(),
            bucket: "bucket".into(),
            prefix: String::new(),
            virtual_hosted: false,
            allow_http: true,
        };
        let snapshot = Snapshot {
            external_id: Uuid::now_v7(),
            revision: 1,
            connection: connection.clone(),
            access_key: "fixture-access".into(),
            secret_key: "fixture-secret".into(),
        };
        let mut report = Report::new(connection);
        report.phase = Phase::ReadAcl;
        report.artifacts.push(Artifact {
            file: TestFile::Write,
            version: Some("original/version".into()),
            upload: None,
            etag: Some("\"content\"".into()),
            cleaned: false,
        });
        let mut recovered = false;
        for _ in 0..6 {
            let next = execute(
                Plan {
                    id,
                    recover: report.blocked.is_some(),
                    report: report.clone(),
                },
                &snapshot,
            )
            .await?;
            report.advance(&next)?;
            report = next;
            if mode == "foreign" {
                assert_eq!(report.blocked, Some(Problem::IdentityConflict));
                assert_eq!(state.writes.load(Relaxed), 0);
                break;
            }
            if report.blocked.is_some() {
                assert_eq!(mode, "uncertain");
                assert_eq!(report.phase, Phase::PublicRead);
                assert_eq!(report.blocked, Some(Problem::UncertainResult));
                assert!(!recovered);
                recovered = true;
            }
            if report.phase == Phase::StartMultipart {
                break;
            }
        }
        if mode != "foreign" {
            assert_eq!(report.phase, Phase::StartMultipart, "{mode}");
            assert_eq!(recovered, mode == "uncertain");
        }
        for (cap, state) in [
            Cap::AclRead,
            Cap::AclWrite,
            Cap::PublicRead,
            Cap::PrivateAccess,
        ]
        .into_iter()
        .zip(expected)
        {
            assert_eq!(report.checks[&cap].state, state, "{mode}: {cap:?}");
        }
        server.abort();
    }
    Ok(())
}
