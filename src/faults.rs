//! Deterministic crash boundaries for isolated integration tests. Absent from default builds.
use std::{path::PathBuf, time::Duration};

fn armed(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("MEDIA_GATEWAY_TEST_FAULT_DIR")?).join(name);
    if !path.exists() {
        return None;
    }
    std::fs::write(path.with_extension("hit"), b"reached").expect("record test boundary");
    Some(path)
}
pub async fn point(name: &str) {
    if let Some(path) = armed(name) {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
pub fn blocking(name: &str) {
    if let Some(path) = armed(name) {
        while path.exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
