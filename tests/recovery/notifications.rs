use super::{Backend, migration, queries, transactions};
use crate::{
    authorization::Principal,
    content::codec::Key,
    model::{RuntimeSettingsStore, cluster::ClusterStore, notification::Consumer, session::Open},
};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_notifications_survive_rollback_takeover_and_membership_changes() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url.trim())
        .await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    let backend = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [73; 32])?;
    let config =
        toml::from_str::<crate::config::FileConfig>("[database]\ntype='postgres'\n")?.into_config();
    RuntimeSettingsStore::ensure(&backend, &crate::model::runtime_defaults(&config)).await?;
    let control = backend
        .register(
            "control",
            "control",
            crate::cluster::identity::credential()?,
            &key,
        )
        .await?
        .unwrap();
    let admin = Principal::Node {
        id: control.node_id,
        role: "control".into(),
    };
    let node = backend
        .provision_node(&admin, "reader", "data", &key)
        .await?;
    let hello = Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    let session = backend.open_session(&node, &key, hello).await?;
    let mut listener = backend.notification_changes().await?;
    let consumer = Consumer {
        node: node.node_id,
        generation: hello.connection,
    };
    let storage = Uuid::now_v7();
    sqlx::query("INSERT INTO storages(id,name,type,endpoint_url,remote_bucket_name,region,access_key_encrypted,secret_key_encrypted,identity_marker) VALUES($1,'source','s3','http://backend:8333','notification-fixture','us-east-1',$2,$3,$4)")
        .bind(storage).bind(crate::content::codec::protect(b"access", &key, format!("storage-{storage}-access").as_bytes())?)
        .bind(crate::content::codec::protect(b"secret", &key, format!("storage-{storage}-secret").as_bytes())?)
        .bind(crate::storage::model::identity_marker(node.deployment_id, storage)?).execute(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let bucket: Uuid = sqlx::query_scalar(
        "INSERT INTO buckets(name,storage_id) VALUES('notifications',$1) RETURNING id",
    )
    .bind(storage)
    .fetch_one(&pool)
    .await?;
    let stream: Uuid = sqlx::query_scalar("INSERT INTO streams(bucket_id,object_key,stream_type,state,size,etag,public_read,metadata) VALUES($1,'object','object','ready',12,'same-etag',true,'{}') RETURNING id").bind(bucket).fetch_one(&pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'object',$2,uuidv7())").bind(bucket).bind(stream).execute(&mut *tx).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notifications")
            .fetch_one(&pool)
            .await?,
        0
    );
    tx.rollback().await?;
    assert!(backend.claim_notifications(&[consumer]).await?.is_empty());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), listener.next())
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES($1,'object',$2,uuidv7())").bind(bucket).bind(stream).execute(&pool).await?;
    let first = backend.claim_notifications(&[consumer]).await?;
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), listener.next()).await??,
        crate::model::notification::Signal::Changes
    );
    assert_eq!(first.len(), 1);
    assert!(backend.claim_notifications(&[consumer]).await?.is_empty());
    assert_eq!(
        backend
            .acknowledge_notifications(
                Consumer {
                    generation: Uuid::new_v4(),
                    ..consumer
                },
                &[first[0].id]
            )
            .await?,
        0
    );
    sqlx::query(
        "UPDATE notification_targets SET claim_until=clock_timestamp()-interval '1 second'",
    )
    .execute(&pool)
    .await?;
    let retried = transactions::notifications::claim(&pool, &[consumer]).await?;
    assert_eq!(retried[0].id, first[0].id);
    assert_eq!(
        backend
            .acknowledge_notifications(consumer, &[retried[0].id])
            .await?,
        1
    );
    assert_eq!(
        backend
            .acknowledge_notifications(consumer, &[retried[0].id])
            .await?,
        0
    );
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE streams SET public_read=false WHERE id=$1")
        .bind(stream)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE streams SET metadata='{\"content_type\":\"text/plain\"}' WHERE id=$1")
        .bind(stream)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let acl = backend.claim_notifications(&[consumer]).await?;
    assert_eq!(
        acl.len(),
        1,
        "same transaction coalesces object changes even with unchanged ETag"
    );
    let connected = backend
        .open_session(
            &node,
            &key,
            Open {
                connection: Uuid::new_v4(),
                previous_epoch: session.epoch,
                ..hello
            },
        )
        .await?;
    assert_eq!(connected.epoch, session.epoch + 1);
    assert_eq!(
        backend
            .acknowledge_notifications(consumer, &[acl[0].id])
            .await?,
        0
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notification_targets WHERE applied_at IS NULL AND retired_at IS NULL").fetch_one(&pool).await?, 0);

    // An activation waiting on publication must not hold a row publication needs.
    let mut publisher = pool.begin().await?;
    sqlx::query("SELECT mokyu_notify_change('bucket',$1)")
        .bind(bucket)
        .execute(&mut *publisher)
        .await?;
    let other = pool.clone();
    let activating = tokio::spawn(async move {
        let mut tx = other.begin().await?;
        sqlx::query("SELECT id FROM nodes WHERE id=$1 FOR UPDATE")
            .bind(consumer.node)
            .execute(&mut *tx)
            .await?;
        transactions::notifications::activate(
            &mut tx,
            Consumer {
                generation: Uuid::new_v4(),
                ..consumer
            },
            "data",
        )
        .await?;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!activating.is_finished());
    sqlx::query("SELECT mokyu_notify_change('object',$1,'another')")
        .bind(bucket)
        .execute(&mut *publisher)
        .await?;
    publisher.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(3), activating).await???;

    let latest = Consumer {
        generation: Uuid::new_v4(),
        ..consumer
    };
    let mut activation = pool.begin().await?;
    transactions::notifications::activate(&mut activation, latest, "data").await?;
    let other = pool.clone();
    let publishing = tokio::spawn(async move {
        sqlx::query("SELECT mokyu_notify_change('object',$1,'joined')")
            .bind(bucket)
            .execute(&other)
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!publishing.is_finished());
    activation.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(3), publishing).await???;
    let target: Uuid = sqlx::query_scalar("SELECT t.generation FROM notification_targets t JOIN notifications n ON n.id=t.notification_id WHERE n.object_key='joined'").fetch_one(&pool).await?;
    assert_eq!(
        target, latest.generation,
        "publication sees membership committed while it waited"
    );

    while tokio::time::timeout(std::time::Duration::from_millis(20), listener.next())
        .await
        .is_ok()
    {}
    let killed: Vec<bool> = sqlx::query_scalar("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'LISTEN%mokyu_notifications%'").fetch_all(&pool).await?;
    assert_eq!(killed, vec![true]);
    let interruption =
        tokio::time::timeout(std::time::Duration::from_secs(3), listener.next()).await?;
    assert!(
        !matches!(
            interruption,
            Ok(crate::model::notification::Signal::Changes)
        ),
        "subscription interruption must be observable"
    );
    if interruption.is_err() {
        listener = backend.notification_changes().await?;
    }
    sqlx::query("SELECT pg_notify('mokyu_notifications','recovered')")
        .execute(&pool)
        .await?;
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), listener.next()).await??,
        crate::model::notification::Signal::Changes
    );

    sqlx::query(
        "UPDATE notification_consumers SET expires_at=clock_timestamp()-interval '1 second'",
    )
    .execute(&pool)
    .await?;
    backend.maintain_notifications().await?;
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notification_targets WHERE applied_at IS NULL AND retired_at IS NULL").fetch_one(&pool).await?, 0);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM notifications")
        .fetch_one(&pool)
        .await?;
    sqlx::query(
        "UPDATE notification_consumers SET expires_at=clock_timestamp()+interval '90 seconds'",
    )
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='single'")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1")
        .bind(bucket)
        .execute(&pool)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notifications")
            .fetch_one(&pool)
            .await?,
        count
    );
    Ok(())
}
