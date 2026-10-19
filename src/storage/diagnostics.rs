use crate::content::codec::IntegrityError;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub(super) struct Diagnostics {
    backend: [AtomicU64; 4],
    content: [AtomicU64; 6],
    pub cache_missing: AtomicU64,
    pub cache_invalid: AtomicU64,
    pub cache_fallback: AtomicU64,
    pub pending_unavailable: AtomicU64,
}

impl Diagnostics {
    pub fn backend(&self, error: &object_store::Error) {
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
        let mut timeout = false;
        while let Some(error) = cause {
            timeout |= error
                .downcast_ref::<reqwest::Error>()
                .is_some_and(|e| e.is_timeout())
                || error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::TimedOut);
            cause = error.source();
        }
        let index = match error {
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => 0,
            _ if timeout => 1,
            object_store::Error::NotFound { .. } => 2,
            _ => 3,
        };
        self.backend[index].fetch_add(1, Relaxed);
    }

    pub fn content(&self, error: &anyhow::Error) {
        let index = match error.downcast_ref::<IntegrityError>() {
            Some(IntegrityError::Length) => 0,
            Some(IntegrityError::Authentication) => 1,
            Some(IntegrityError::Decompression) => 2,
            Some(IntegrityError::Hash) => 3,
            Some(IntegrityError::Metadata | IntegrityError::RawMetadata) => 4,
            Some(IntegrityError::MissingKey) => 5,
            Some(IntegrityError::RemoteMissing) => {
                self.backend[2].fetch_add(1, Relaxed);
                return;
            }
            _ => return,
        };
        self.content[index].fetch_add(1, Relaxed);
    }

    pub fn snapshot(&self) -> Value {
        let backend = self.backend.each_ref().map(|v| v.load(Relaxed));
        let content = self.content.each_ref().map(|v| v.load(Relaxed));
        json!({
            "backend": {"permission": backend[0], "timeout": backend[1], "missing": backend[2], "other": backend[3]},
            "content": {"length": content[0], "authentication": content[1], "decompression": content[2], "hash": content[3], "metadata": content[4], "missing_key": content[5]},
            "cache": {"missing": self.cache_missing.load(Relaxed), "invalid": self.cache_invalid.load(Relaxed), "fallback": self.cache_fallback.load(Relaxed)},
            "pending_unavailable": self.pending_unavailable.load(Relaxed)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_transport_and_content_without_parsing_messages() {
        let diagnostics = Diagnostics::default();
        diagnostics.backend(&object_store::Error::NotFound {
            path: "test".into(),
            source: std::io::Error::from(std::io::ErrorKind::NotFound).into(),
        });
        diagnostics.backend(&object_store::Error::PermissionDenied {
            path: "test".into(),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
        });
        diagnostics.backend(&object_store::Error::Generic {
            store: "test",
            source: std::io::Error::from(std::io::ErrorKind::TimedOut).into(),
        });
        diagnostics.backend(&object_store::Error::Generic {
            store: "test",
            source: std::io::Error::other("timeout permission missing").into(),
        });
        for error in [
            IntegrityError::Length,
            IntegrityError::Authentication,
            IntegrityError::Decompression,
            IntegrityError::Hash,
            IntegrityError::Metadata,
            IntegrityError::MissingKey,
        ] {
            diagnostics.content(&anyhow::Error::from(error).context("wrapped"));
        }
        diagnostics.content(&anyhow::anyhow!("hash_mismatch"));
        let snapshot = diagnostics.snapshot();
        assert!(
            snapshot["backend"]
                .as_object()
                .unwrap()
                .values()
                .all(|v| *v == 1)
        );
        assert!(
            snapshot["content"]
                .as_object()
                .unwrap()
                .values()
                .all(|v| *v == 1)
        );
        assert_eq!(snapshot["pending_unavailable"], 0);
    }
}
