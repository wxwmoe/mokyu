use super::{Config as Rules, Start};
use crate::{
    authorization::{Permit, Principal},
    model::{
        Stores, StreamCreate, StreamPurpose, TaskClaim, bucket::CreateBucket, object::ObjectChange,
        task::Execution,
    },
    storage::model::Record,
};
use anyhow::Result;
use serde_json::json;
use uuid::Uuid;

async fn task(
    stores: &Stores,
    bucket: Uuid,
    storage: Uuid,
    rule: Uuid,
    revision: i64,
    key: &str,
) -> Result<Execution> {
    let request = || Start {
        bucket,
        storage,
        rule,
        revision,
        key: key.into(),
        task: Uuid::now_v7(),
    };
    let (a, b) = tokio::join!(
        stores.mirroring.request(request()),
        stores.mirroring.request(request())
    );
    let id = a?;
    assert_eq!(id, b?, "concurrent readers must share a fill");
    let token = Uuid::now_v7();
    assert_eq!(
        stores
            .task_execution
            .claim(TaskClaim {
                draining: false,
                class: 2,
                active: &[],
                token
            })
            .await?,
        Some(id)
    );
    let task = Execution { id, token };
    stores.mirroring.bind_source(task, None).await?;
    Ok(task)
}

fn stream(bucket: Uuid, key: &str, task: Option<Execution>) -> StreamCreate {
    StreamCreate {
        authority: Permit::system(),
        bucket,
        key: key.into(),
        purpose: if task.is_some() {
            StreamPurpose::Mirror
        } else {
            StreamPurpose::Object
        },
        metadata: json!({}),
        public: false,
        completion: None,
        owner: task.map(|t| (t.id, t.token)),
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_CONFIG pointing to a fresh isolated database"]
async fn mirroring_database_contract() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let (config, mut secrets, budget) =
        crate::config::Config::load(std::path::Path::new(&std::env::var("MOKYU_TEST_CONFIG")?))?;
    let (stores, _owner) = crate::model::connect(&config, &mut secrets, &budget, "single").await?;
    stores.runtime.initialize(false).await?;
    let mut creation = stores.storage_records.creation().await?;
    let storage = Uuid::now_v7();
    let record = Record {
        id: storage,
        name: "origin-target".into(),
        r#type: "local".into(),
        path: Some(format!("/tmp/{storage}")),
        endpoint_url: None,
        remote_bucket_name: None,
        region: None,
        prefix: String::new(),
        access_key_encrypted: None,
        secret_key_encrypted: None,
        identity_marker: crate::storage::model::identity_marker(creation.deployment, storage)?,
        enabled: true,
    };
    creation
        .writer
        .prepare(&record, None, None, None, None, None)
        .await?;
    creation.writer.finish().await?;
    let bucket = stores
        .buckets
        .create(
            &Principal::Local,
            CreateBucket {
                name: "origin-contract".into(),
                project_id: None,
                storage_id: Some(storage),
            },
        )
        .await?
        .id;
    let rule = Uuid::now_v7();
    let mut rules: Rules = serde_json::from_value(
        json!({"enabled":true,"rules":[{"id":rule,"name":"Origin","source":{"type":"http","url":"https://origin.example"}}]}),
    )?;
    let save = |revision, rules: Rules| {
        let stores = &stores;
        async move {
            stores
                .mirroring
                .save(&Principal::Local, bucket, revision, &rules, vec![1, 2, 3])
                .await
        }
    };
    assert_eq!(save(0, rules.clone()).await?, 1);
    assert!(
        save(0, rules.clone()).await.is_err(),
        "stale settings cannot overwrite a newer rule"
    );

    let good = task(&stores, bucket, storage, rule, 1, "saved").await?;
    let id = stores
        .streams
        .create(stream(bucket, "saved", Some(good)))
        .await?;
    assert!(
        stores
            .streams
            .finish(id, 0, "d41d8cd98f00b204e9800998ecf8427e", json!({}))
            .await?
    );
    assert!(stores.publish.publish(id, None, None, None).await?);
    stores.task_control.complete(good).await?;
    assert_eq!(stores.streams.current(bucket, "saved").await?.id, id);

    let replaced = task(&stores, bucket, storage, rule, 1, "replace").await?;
    let old = stores
        .streams
        .create(stream(bucket, "replace", Some(replaced)))
        .await?;
    let new = stores
        .streams
        .create(stream(bucket, "replace", None))
        .await?;
    stores.streams.finish(old, 0, "old", json!({})).await?;
    assert!(stores.publish.publish(old, None, None, None).await.is_err());
    stores.streams.finish(new, 0, "new", json!({})).await?;
    assert!(stores.publish.publish(new, None, None, None).await?);
    stores.task_control.complete(replaced).await?;

    let removed = task(&stores, bucket, storage, rule, 1, "removed").await?;
    let old = stores
        .streams
        .create(stream(bucket, "removed", Some(removed)))
        .await?;
    stores
        .objects
        .change(ObjectChange {
            principal: Principal::Local,
            bucket,
            key: "removed".into(),
            expected: None,
            public: None,
            if_match: None,
        })
        .await?;
    stores.streams.finish(old, 0, "old", json!({})).await?;
    assert!(stores.publish.publish(old, None, None, None).await.is_err());
    assert!(
        stores
            .mirroring
            .request(Start {
                bucket,
                storage,
                rule,
                revision: 1,
                key: "removed".into(),
                task: Uuid::now_v7()
            })
            .await
            .is_err()
    );
    stores.task_control.complete(removed).await?;

    let stale = task(&stores, bucket, storage, rule, 1, "stale-rule").await?;
    let old = stores
        .streams
        .create(stream(bucket, "stale-rule", Some(stale)))
        .await?;
    rules.enabled = false;
    assert_eq!(save(1, rules.clone()).await?, 2);
    stores.streams.finish(old, 0, "old", json!({})).await?;
    assert!(stores.publish.publish(old, None, None, None).await.is_err());
    rules.enabled = true;
    assert_eq!(save(2, rules.clone()).await?, 3);
    assert!(
        stores
            .mirroring
            .request(Start {
                bucket,
                storage,
                rule,
                revision: 3,
                key: "removed".into(),
                task: Uuid::now_v7()
            })
            .await
            .is_err(),
        "reenabling keeps delete suppression"
    );
    stores.task_control.complete(stale).await?;

    stores
        .streams
        .create(stream(bucket, "ordinary-upload", None))
        .await?;
    assert!(
        stores
            .mirroring
            .request(Start {
                bucket,
                storage,
                rule,
                revision: 3,
                key: "ordinary-upload".into(),
                task: Uuid::now_v7()
            })
            .await
            .is_err()
    );
    Ok(())
}
