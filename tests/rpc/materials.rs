use super::*;

fn value(deployment: Uuid, id: &str, json: &[u8]) -> wire::Material {
    let version = read::Version::new(deployment, read::MaterialType::Bucket, id.into(), json);
    wire::Material {
        version: Some(wire::Cached {
            r#type: wire::MaterialType::Bucket as i32,
            id: id.into(),
            revision: version.revision.to_vec(),
        }),
        value: Some(wire::material::Value::Json(json.to_vec())),
    }
}

#[test]
fn declared_material_survives_eviction_until_last_request_releases_it() {
    let deployment = Uuid::new_v4();
    let cache = Cache {
        index: Mutex::new(Index::default()),
        memory: Arc::new(Semaphore::new(1)),
        public: Weak::new(),
    };
    let supplied = value(deployment, "bucket", br#"{"revision":1}"#);
    let version = supplied.version.clone().unwrap();
    let first = cache
        .apply(deployment, Held::new(), vec![supplied])
        .unwrap();
    assert_eq!(cache.memory.available_permits(), 0);
    let declared = cache.snapshot();
    cache.index.lock().unwrap().values.clear();
    cache.index.lock().unwrap().order.clear();
    drop(first);
    assert_eq!(cache.memory.available_permits(), 0);
    let reused = cache
        .apply(
            deployment,
            declared,
            vec![wire::Material {
                version: Some(version.clone()),
                value: Some(wire::material::Value::Reuse(true)),
            }],
        )
        .unwrap();
    let parsed: serde_json::Value = material(&reused, &version).unwrap();
    assert_eq!(parsed["revision"], 1);
    drop(reused);
    assert_eq!(cache.memory.available_permits(), 1);
}

#[test]
fn material_replies_validate_revision_deployment_and_reuse_before_admission() {
    let deployment = Uuid::new_v4();
    let cache = Cache::default();
    let valid = value(deployment, "one", b"{}");
    let mut wrong = value(deployment, "two", b"{}");
    wrong.value = Some(wire::material::Value::Json(b"[]".to_vec()));
    assert!(
        cache
            .apply(deployment, Held::new(), vec![valid.clone(), wrong])
            .is_err()
    );
    assert!(cache.snapshot().is_empty());
    assert!(
        cache
            .apply(Uuid::new_v4(), Held::new(), vec![valid.clone()])
            .is_err()
    );
    assert!(
        cache
            .apply(deployment, Held::new(), vec![valid.clone(), valid.clone()])
            .is_err()
    );
    let reuse = wire::Material {
        version: valid.version.clone(),
        value: Some(wire::material::Value::Reuse(true)),
    };
    assert!(
        cache
            .apply(deployment, Held::new(), vec![reuse.clone()])
            .is_err()
    );
    let held = cache.apply(deployment, Held::new(), vec![valid]).unwrap();
    let mut changed = reuse;
    changed.version.as_mut().unwrap().revision[0] ^= 1;
    assert!(cache.apply(deployment, held, vec![changed]).is_err());
}

#[test]
fn cache_pressure_skips_admission_without_losing_held_materials() {
    let deployment = Uuid::new_v4();
    let cache = Cache {
        index: Mutex::new(Index::default()),
        memory: Arc::new(Semaphore::new(4)),
        public: Weak::new(),
    };
    let values = (0..4)
        .map(|index| value(deployment, &index.to_string(), b"{}"))
        .collect();
    let held = cache.apply(deployment, Held::new(), values).unwrap();
    assert_eq!(cache.memory.available_permits(), 0);
    let additional = cache
        .apply(
            deployment,
            cache.snapshot(),
            vec![value(deployment, "extra", b"{}")],
        )
        .unwrap();
    assert_eq!(additional.len(), 1);
    assert!(cache.snapshot().is_empty());
    assert_eq!(cache.memory.available_permits(), 0);
    assert_eq!(held.len(), 4);
    drop(held);
    assert_eq!(cache.memory.available_permits(), 4);
}

#[test]
fn late_read_policy_keeps_its_reply_without_replacing_current_policy() {
    let deployment = Uuid::new_v4();
    let storage = Uuid::new_v4();
    let cache = Cache::default();
    let policy = |revision| {
        let value = read::Policy {
            storage_id: storage,
            revision,
            cors_cache_enabled: true,
            public_cache_enabled: true,
            refresh_seconds: 1,
            max_age_seconds: 5,
            read_integrity: "inline".into(),
        };
        let json = serde_json::to_vec(&value).unwrap();
        wire::Material {
            version: Some(crate::rpc::read::version(read::Version::new(
                deployment,
                read::MaterialType::Policy,
                storage.to_string(),
                &json,
            ))),
            value: Some(wire::material::Value::Json(json)),
        }
    };
    cache
        .apply(deployment, Held::new(), vec![policy(2)])
        .unwrap();
    let old = policy(1);
    let version = old.version.clone().unwrap();
    let held = cache.apply(deployment, Held::new(), vec![old]).unwrap();
    assert_eq!(
        material::<read::Policy>(&held, &version).unwrap().revision,
        1
    );
    assert_eq!(cache.policy(storage).unwrap().revision, 2);
    assert_eq!(cache.snapshot().len(), 1);
}
