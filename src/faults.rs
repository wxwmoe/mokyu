//! Deterministic crash boundaries for isolated integration tests. Absent from default builds.
use std::{path::PathBuf, time::Duration};

tokio::task_local! {
    static DEFERRED: std::cell::RefCell<Vec<String>>;
}

pub async fn defer<F: std::future::Future>(work: F) -> F::Output {
    let (result, points) = DEFERRED
        .scope(std::cell::RefCell::new(Vec::new()), async {
            let result = work.await;
            let points = DEFERRED.with(|queue| queue.take());
            (result, points)
        })
        .await;
    for name in points {
        point(&name).await;
    }
    result
}

pub fn requested(name: &str) -> bool {
    std::env::var_os("MOKYU_TEST_FAULT_DIR")
        .is_some_and(|path| PathBuf::from(path).join(name).exists())
}

fn armed(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("MOKYU_TEST_FAULT_DIR")?).join(name);
    if !path.exists() {
        return None;
    }
    std::fs::write(path.with_extension("hit"), b"reached").expect("record test boundary");
    Some(path)
}
pub async fn point(name: &str) {
    if DEFERRED
        .try_with(|queue| queue.borrow_mut().push(name.to_owned()))
        .is_ok()
    {
        return;
    }
    if let Some(path) = armed(name) {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
pub fn blocking(name: &str) {
    if let Some(path) = armed(name) {
        if std::fs::read(&path).is_ok_and(|value| value == b"panic") {
            panic!("injected blocking failure: {name}");
        }
        while path.exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
