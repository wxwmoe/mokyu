use super::{Backend, migration};
use crate::{
    cluster::NodeState,
    content::codec,
    model::{TaskClaim, TaskFinish, TaskLeaseStore, task::Execution},
};
use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

fn claimant<'a>(node: &'a NodeState, key: &'a codec::Key, token: Uuid) -> TaskClaim<'a> {
    TaskClaim {
        class: 0,
        active: &[],
        node: Some(node),
        role: "worker",
        credential_key: key,
        token,
    }
}

#[tokio::test]
#[ignore = "requires MOKYU_TEST_DATABASE_URL_FILE pointing to a new isolated PostgreSQL database"]
async fn postgres_task_claim_recovery() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPool::connect(url.trim()).await?;
    assert!(
        super::queries::metadata_table()
            .fetch_one(&pool)
            .await?
            .is_none()
    );
    {
        let mut owner = pool.acquire().await?;
        migration::run(&pool, &mut owner, false).await?;
    }
    let first = Backend::new(pool.clone());
    let second = Backend::new(pool.clone());
    let key = codec::Key::new("aes-256-gcm", [7; 32])?;
    let nodes = [
        NodeState {
            node_id: Uuid::new_v4(),
            credential: "first".into(),
        },
        NodeState {
            node_id: Uuid::new_v4(),
            credential: "second".into(),
        },
    ];
    for node in &nodes {
        let encrypted = codec::protect(
            node.credential.as_bytes(),
            &key,
            crate::cluster::credential_binding(node.node_id).as_bytes(),
        )?;
        sqlx::query(
            "INSERT INTO nodes(id,name,role,credential_encrypted) VALUES($1,$2,'worker',$3)",
        )
        .bind(node.node_id)
        .bind(node.node_id.to_string())
        .bind(encrypted)
        .execute(&pool)
        .await?;
    }
    let task = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state) VALUES($1,'integrity','queued')")
        .bind(task)
        .execute(&pool)
        .await?;
    let tokens = [Uuid::new_v4(), Uuid::new_v4()];
    let (a, b) = tokio::join!(
        first.claim(claimant(&nodes[0], &key, tokens[0])),
        second.claim(claimant(&nodes[1], &key, tokens[1]))
    );
    let results = [a?, b?];
    assert_eq!(results.iter().filter(|id| id.is_some()).count(), 1);
    let winner = results.iter().position(|id| id.is_some()).unwrap();
    assert_eq!(results[winner], Some(task));
    let record = first.load(task).await?;
    assert_eq!(record.state, "running");
    assert_eq!(record.lease_token, Some(tokens[winner]));
    assert!(!first.renew(task, tokens[1 - winner]).await?);

    let bucket = Uuid::new_v4();
    let stream = Uuid::new_v4();
    let storage = Uuid::new_v4();
    sqlx::query("INSERT INTO storages(id,name,type,path) VALUES($1,'task-claim','local','/data/task-claim')")
        .bind(storage).execute(&pool).await?;
    sqlx::query("INSERT INTO buckets(id,name,storage_id) VALUES($1,'task-claim',$2)")
        .bind(bucket)
        .bind(storage)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,owner_task_id,owner_lease_token) VALUES($1,$2,'owned','object','writing',$3,$4)")
        .bind(stream).bind(bucket).bind(task).bind(tokens[winner]).execute(&pool).await?;
    sqlx::query("UPDATE tasks SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
        .bind(task)
        .execute(&pool)
        .await?;
    assert_eq!(second.recover().await?, 1);
    assert_eq!(second.recover().await?, 0);
    assert_eq!(first.load(task).await?.state, "queued");
    let abandoned: (String, Option<Uuid>, Option<Uuid>) =
        sqlx::query_as("SELECT state,owner_task_id,owner_lease_token FROM streams WHERE id=$1")
            .bind(stream)
            .fetch_one(&pool)
            .await?;
    assert_eq!(abandoned, ("abandoned".into(), None, None));

    let mut input = claimant(&nodes[0], &key, Uuid::new_v4());
    let active = [task];
    input.active = &active;
    assert_eq!(first.claim(input).await?, None);
    sqlx::query("UPDATE tasks SET execution_scope='node',target_node_id=$2 WHERE id=$1")
        .bind(task)
        .bind(nodes[0].node_id)
        .execute(&pool)
        .await?;
    assert_eq!(
        second
            .claim(claimant(&nodes[1], &key, Uuid::new_v4()))
            .await?,
        None
    );
    let invalid = NodeState {
        node_id: nodes[0].node_id,
        credential: "invalid".into(),
    };
    assert!(
        first
            .claim(claimant(&invalid, &key, Uuid::new_v4()))
            .await
            .is_err()
    );
    assert_eq!(first.load(task).await?.state, "queued");
    sqlx::query("UPDATE nodes SET enabled=false WHERE id=$1")
        .bind(nodes[0].node_id)
        .execute(&pool)
        .await?;
    assert!(
        first
            .claim(claimant(&nodes[0], &key, Uuid::new_v4()))
            .await
            .is_err()
    );
    assert_eq!(first.load(task).await?.state, "queued");
    sqlx::query("UPDATE nodes SET enabled=true WHERE id=$1")
        .bind(nodes[0].node_id)
        .execute(&pool)
        .await?;
    let token = Uuid::new_v4();
    assert_eq!(
        first.claim(claimant(&nodes[0], &key, token)).await?,
        Some(task)
    );
    assert!(first.renew(task, token).await?);
    assert!(!first.renew(task, tokens[winner]).await?);
    let parent = Uuid::new_v4();
    sqlx::query("INSERT INTO tasks(id,task_type,state) VALUES($1,'integrity','queued')")
        .bind(parent)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE tasks SET parent_task_id=$2 WHERE id=$1")
        .bind(task)
        .bind(parent)
        .execute(&pool)
        .await?;
    let failure = |token| TaskFinish {
        execution: Execution { id: task, token },
        reuse_seconds: 60,
        error: Some("interrupted batch".into()),
    };
    first.finish(failure(tokens[winner])).await?;
    assert_eq!(first.load(task).await?.state, "running");
    assert_eq!(first.load(parent).await?.state, "queued");
    sqlx::query("UPDATE tasks SET state='paused' WHERE id=$1 OR id=$2")
        .bind(task)
        .bind(parent)
        .execute(&pool)
        .await?;
    first.finish(failure(token)).await?;
    assert_eq!(first.load(task).await?.state, "paused");
    assert_eq!(first.load(parent).await?.state, "paused");
    let resumed = Uuid::new_v4();
    sqlx::query("UPDATE tasks SET state='running',lease_token=$3,lease_expires_at=now()+interval '5 minutes' WHERE id=$1 OR id=$2")
        .bind(task)
        .bind(parent).bind(resumed)
        .execute(&pool)
        .await?;
    first.finish(failure(token)).await?;
    assert_eq!(first.load(task).await?.state, "running");
    first.finish(failure(resumed)).await?;
    assert_eq!(first.load(task).await?.state, "failed");
    assert_eq!(first.load(parent).await?.state, "failed");
    Ok(())
}
