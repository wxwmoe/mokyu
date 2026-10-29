use super::*;
use std::sync::atomic::AtomicBool;

fn run() -> Arc<RunLease> {
    Arc::new(RunLease {
        run: wire::Run {
            boot_id: Uuid::new_v4().to_string(),
            fence: Uuid::new_v4().to_string(),
        },
        expires: Mutex::new(Instant::now() + Duration::from_secs(90)),
        invalidated: AtomicBool::new(false),
    })
}

fn grant() -> wire::Grant {
    wire::Grant {
        r#type: wire::GrantType::StorageRead as i32,
        id: Uuid::new_v4().to_string(),
        storage_id: Uuid::new_v4().to_string(),
        valid_for_ms: 60_000,
    }
}

#[test]
fn external_work_grants_keep_run_and_release_guards_without_internal_storage() {
    let resources = Arc::new(Resources::default());
    let run = run();
    let mut value = grant();
    value.storage_id.clear();
    assert!(
        resources
            .adopt(&run, &[value.clone()], Instant::now())
            .is_err()
    );
    value.r#type = wire::GrantType::TaskWork as i32;
    let held = resources
        .adopt(&run, &[value.clone()], Instant::now())
        .unwrap();
    held[0].check().unwrap();
    drop(held);
    let batch = resources.batch(&run);
    assert_eq!(batch.release.len(), 1);
    acknowledge(&resources, &run, &batch);
    value.id = Uuid::new_v4().to_string();
    let held = resources.adopt(&run, &[value], Instant::now()).unwrap();
    run.invalidate();
    assert!(held[0].check().is_err());
    drop(held);
    assert!(resources.batch(&run).release.is_empty());
    assert!(resources.entries.lock().unwrap().is_empty());
}

#[test]
fn resource_deadlines_follow_remaining_ttl_and_release_immediately() {
    let resources = Arc::new(Resources::default());
    let run = run();
    let now = Instant::now();
    assert_eq!(
        renew_at(now, now + Duration::from_secs(90)),
        now + Duration::from_secs(30)
    );
    assert_eq!(
        renew_at(now, now + Duration::from_secs(6)),
        now + Duration::from_secs(2)
    );
    assert_eq!(renew_at(now, now), now);
    let held = resources.adopt(&run, &[grant()], now).unwrap();
    assert!(resources.next(&run) > now + Duration::from_secs(15));
    assert!(resources.batch(&run).renew.is_empty());
    drop(held);
    assert_eq!(resources.batch(&run).release.len(), 1);
}

fn acknowledge(resources: &Resources, run: &Arc<RunLease>, batch: &Batch) {
    let results = batch
        .renew
        .iter()
        .map(|reference| wire::GrantResult {
            grant: Some(reference.clone()),
            accepted: true,
            valid_for_ms: 60_000,
        })
        .chain(batch.release.iter().map(|reference| wire::GrantResult {
            grant: Some(reference.clone()),
            accepted: true,
            valid_for_ms: 0,
        }))
        .collect::<Vec<_>>();
    resources
        .acknowledge(run, batch, Instant::now(), &results)
        .unwrap();
}

#[test]
fn resources_share_handles_bound_batches_and_release_after_last_owner() {
    let resources = Arc::new(Resources::default());
    let run = run();
    let grants = (0..600).map(|_| grant()).collect::<Vec<_>>();
    let mut held = Vec::new();
    for chunk in grants.chunks(256) {
        held.extend(resources.adopt(&run, chunk, Instant::now()).unwrap());
    }
    let shared = resources.adopt(&run, &grants[..1], Instant::now()).unwrap();
    assert!(Arc::ptr_eq(&shared[0], &held[0]));
    for entry in resources.entries.lock().unwrap().values_mut() {
        entry.next = Instant::now();
    }
    let mut renewed = HashSet::new();
    loop {
        let batch = resources.batch(&run);
        assert!(batch.release.is_empty());
        assert!(batch.renew.len() <= 256);
        if batch.renew.is_empty() {
            break;
        }
        for reference in &batch.renew {
            assert!(renewed.insert(reference.id.clone()));
        }
        acknowledge(&resources, &run, &batch);
    }
    assert_eq!(renewed.len(), 600);
    drop(held);
    let mut released = 0;
    loop {
        let batch = resources.batch(&run);
        assert!(batch.renew.is_empty());
        if batch.release.is_empty() {
            break;
        }
        released += batch.release.len();
        acknowledge(&resources, &run, &batch);
    }
    assert_eq!(released, 599);
    shared[0].check().unwrap();
    drop(shared);
    let batch = resources.batch(&run);
    assert_eq!(batch.release.len(), 1);
    acknowledge(&resources, &run, &batch);
    assert!(resources.entries.lock().unwrap().is_empty());
}

#[test]
fn expired_rejected_and_released_resources_cannot_be_revived() {
    let resources = Arc::new(Resources::default());
    let run = run();
    let grants = [grant()];
    let held = resources.adopt(&run, &grants, Instant::now()).unwrap();
    let id = Uuid::parse_str(&grants[0].id).unwrap();
    resources.entries.lock().unwrap().get_mut(&id).unwrap().next = Instant::now();
    let batch = resources.batch(&run);
    assert_eq!(batch.renew.len(), 1);
    *held[0].expires.lock().unwrap() = Instant::now() - Duration::from_secs(1);
    acknowledge(&resources, &run, &batch);
    assert!(held[0].check().is_err());
    assert!(resources.adopt(&run, &grants, Instant::now()).is_err());
    drop(held);
    let batch = resources.batch(&run);
    assert_eq!(batch.release.len(), 1);
    assert!(resources.adopt(&run, &grants, Instant::now()).is_err());
    // A failed transport leaves the same release queued for the next session.
    assert_eq!(resources.batch(&run).release, batch.release);
    acknowledge(&resources, &run, &batch);
    assert!(resources.entries.lock().unwrap().is_empty());

    let held = resources.adopt(&run, &[grant()], Instant::now()).unwrap();
    run.invalidate();
    assert!(held[0].check().is_err());
    assert!(resources.adopt(&run, &[grant()], Instant::now()).is_err());
}

#[test]
fn malformed_grants_and_conflicting_reuse_do_not_partially_register() {
    let resources = Arc::new(Resources::default());
    let run = run();
    let granted = grant();
    for invalid in [
        wire::Grant {
            r#type: 0,
            ..granted.clone()
        },
        wire::Grant {
            valid_for_ms: 90_001,
            ..granted.clone()
        },
        wire::Grant {
            storage_id: Uuid::nil().to_string(),
            ..granted.clone()
        },
    ] {
        assert!(
            resources
                .adopt(&run, &[grant(), invalid], Instant::now())
                .is_err()
        );
        assert!(resources.entries.lock().unwrap().is_empty());
    }
    let held = resources
        .adopt(&run, &[granted.clone()], Instant::now())
        .unwrap();
    let conflicting = wire::Grant {
        storage_id: Uuid::new_v4().to_string(),
        ..granted.clone()
    };
    assert!(
        resources
            .adopt(&run, &[grant(), conflicting], Instant::now())
            .is_err()
    );
    assert_eq!(resources.entries.lock().unwrap().len(), 1);
    assert!(
        resources
            .adopt(&run, &[granted.clone(), granted], Instant::now())
            .is_err()
    );
    held[0].check().unwrap();
}
