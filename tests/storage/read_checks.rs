use super::*;

async fn idle(checks: &Checks) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while checks.snapshot()["active"] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(checks.snapshot()["used_bytes"], 0);
}

#[tokio::test]
async fn chunk_checks_verify_bytes_and_hold_job_and_memory_through_completion() {
    let checks = Checks::new(Arc::new(Semaphore::new(4)));
    let release = Arc::new(Semaphore::new(0));
    for valid in [true, false, true, false] {
        let bytes = Bytes::from_static(b"checked bytes");
        let expected = if valid {
            blake3::hash(&bytes).as_bytes().to_vec()
        } else {
            vec![0; 32]
        };
        let release = release.clone();
        checks.chunk(expected, bytes, UNIT, move |result, _bytes| async move {
            assert_eq!(result, valid);
            release.acquire().await.unwrap().forget();
        });
    }
    let overflow = checks.chunk(vec![0; 32], Bytes::new(), UNIT, |_, _| async {
        panic!("full job pool must skip");
    });
    assert!(!overflow.cacheable());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while checks.snapshot()["verified"] != 2 || checks.snapshot()["failed"] != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(checks.snapshot()["active"], 4);
    assert_eq!(checks.snapshot()["used_bytes"], UNIT * 4);
    release.add_permits(4);
    idle(&checks).await;
}

#[tokio::test]
async fn busy_cpu_and_oversized_retention_are_unverified() {
    let checks = Checks::new(Arc::new(Semaphore::new(0)));
    let evidence = checks.chunk(vec![0; 32], Bytes::new(), UNIT, |_, _| async {
        panic!("busy CPU must skip");
    });
    idle(&checks).await;
    assert!(!evidence.cacheable());
    let oversized = checks.chunk(vec![0; 32], Bytes::new(), MEMORY + 1, |_, _| async {
        panic!("memory admission must skip");
    });
    assert!(!oversized.cacheable());
    assert_eq!(checks.snapshot()["skipped"], 2);
}

#[tokio::test]
async fn raw_checks_distinguish_full_body_mismatch_and_cancellation() {
    let checks = Checks::new(Arc::new(Semaphore::new(1)));
    for (length, valid) in [(6, true), (6, false), (7, true)] {
        let expected = if valid {
            Md5::digest(b"abcdef").to_vec()
        } else {
            vec![0; 16]
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut audit = checks.raw(length, Some(expected), move |result| async move {
            let _ = sender.send(result);
        });
        let evidence = audit.evidence.clone();
        audit.feed(Bytes::from_static(b"abc"), UNIT);
        audit.feed(Bytes::from_static(b"def"), UNIT);
        drop(audit);
        idle(&checks).await;
        if length == 6 {
            assert_eq!(receiver.await.unwrap(), valid);
            assert_eq!(evidence.verified(), valid);
            assert_eq!(evidence.cacheable(), valid);
        } else {
            assert!(receiver.await.is_err());
            assert!(!evidence.cacheable());
        }
    }
}

#[tokio::test]
async fn raw_queue_overflow_cannot_become_verified() {
    let checks = Checks::new(Arc::new(Semaphore::new(1)));
    let mut audit = checks.raw(100, Some(vec![0; 16]), |_| async {
        panic!("overflow must skip completion");
    });
    let evidence = audit.evidence.clone();
    for _ in 0..100 {
        audit.feed(Bytes::from_static(b"x"), UNIT);
    }
    drop(audit);
    idle(&checks).await;
    assert!(!evidence.cacheable());
}
