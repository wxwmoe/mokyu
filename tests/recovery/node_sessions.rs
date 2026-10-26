use super::{Backend, migration};
use crate::{
    authorization::Principal,
    content::codec::Key,
    model::{
        RuntimeSettingsStore,
        cluster::ClusterStore,
        session::{Open, Run},
    },
};
use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

async fn expire(pool: &PgPool, node: Uuid) -> Result<()> {
    sqlx::query("UPDATE node_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1")
        .bind(node)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_node_sessions_recover_across_controls_and_fence_old_runs() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    assert!(
        super::queries::metadata_table()
            .fetch_one(&pool)
            .await?
            .is_none()
    );
    {
        let mut owner = pool.acquire().await?;
        migration::run(&mut owner).await?;
    }
    sqlx::query("UPDATE mokyu_meta SET deployment_mode='cluster'")
        .execute(&pool)
        .await?;
    let other = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    let first = Backend::new(pool.clone(), pool.acquire().await?.detach(), false);
    let second = Backend::new(other.clone(), other.acquire().await?.detach(), false);
    let key = Key::new("aes-256-gcm", [73; 32])?;
    let config =
        toml::from_str::<crate::config::FileConfig>("[database]\ntype='postgres'\n")?.into_config();
    RuntimeSettingsStore::ensure(&first, &crate::model::runtime_defaults(&config)).await?;
    let control = first
        .register(
            "control-test",
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
    assert!(
        first
            .provision_node(&Principal::Local, "denied", "data", &key)
            .await
            .is_err()
    );
    let node = first
        .provision_node(&admin, "data-test", "data", &key)
        .await?;
    node.validate()?;
    assert!(
        second
            .provision_node(&admin, "data-test", "data", &key)
            .await
            .is_err()
    );
    assert!(
        first
            .provision_node(&admin, "control-test", "control", &key)
            .await
            .is_err()
    );
    let hello = Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    let initial = first.open_session(&node, &key, hello).await?;
    assert_eq!(initial.epoch, 1);
    assert!(initial.valid_for_ms > 85_000 && initial.valid_for_ms <= 90_000);
    assert_eq!(initial.settings.len(), 5);
    assert!(!initial.settings.iter().any(|(name, _, _)| name == "web"));
    let expires: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT expires_at FROM node_sessions WHERE node_id=$1")
            .bind(node.node_id)
            .fetch_one(&pool)
            .await?;
    let replayed = second.open_session(&node, &key, hello).await?;
    assert_eq!(initial.run.fence, replayed.run.fence);
    assert_eq!(initial.epoch + 1, replayed.epoch);
    assert!(
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT expires_at FROM node_sessions WHERE node_id=$1"
        )
        .bind(node.node_id)
        .fetch_one(&pool)
        .await?
            >= expires
    );
    let reconnect = Open {
        connection: Uuid::new_v4(),
        previous_epoch: replayed.epoch,
        ..hello
    };
    let connected = second.open_session(&node, &key, reconnect).await?;
    assert_eq!(connected.epoch, replayed.epoch + 1);
    assert_eq!(connected.run.fence, initial.run.fence);
    assert!(first.open_session(&node, &key, hello).await.is_err());
    assert!(
        first
            .pulse_session(
                &node,
                &key,
                initial.run,
                initial.epoch,
                1,
                false,
                Default::default()
            )
            .await
            .is_err()
    );
    let pulse = first
        .pulse_session(
            &node,
            &key,
            connected.run,
            connected.epoch,
            1,
            false,
            Default::default(),
        )
        .await?;
    assert_eq!(pulse.run.fence, initial.run.fence);
    assert!(
        second
            .pulse_session(
                &node,
                &key,
                connected.run,
                connected.epoch,
                1,
                false,
                Default::default()
            )
            .await
            .is_err()
    );
    let competing = Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    assert!(first.open_session(&node, &key, competing).await.is_err());
    let bad_run = Run {
        boot: Uuid::new_v4(),
        ..initial.run
    };
    assert!(first.sync_node(&node, &key, bad_run).await.is_err());
    for invalid in [
        crate::cluster::NodeState {
            credential_generation: node.credential_generation + 1,
            ..node.clone()
        },
        crate::cluster::NodeState {
            deployment_id: Uuid::new_v4(),
            ..node.clone()
        },
        crate::cluster::NodeState {
            role: "worker".into(),
            ..node.clone()
        },
        crate::cluster::NodeState {
            credential: crate::cluster::identity::credential()?,
            ..node.clone()
        },
    ] {
        assert!(
            second
                .sync_node(&invalid, &key, connected.run)
                .await
                .is_err()
        );
    }
    // Shared identity checks must not deadlock heartbeat's node status update.
    let parallel = async {
        let reads = futures_util::future::join_all(
            (0..24).map(|_| second.sync_node(&node, &key, connected.run)),
        );
        let (reads, pulse) = tokio::join!(
            reads,
            first.pulse_session(
                &node,
                &key,
                connected.run,
                connected.epoch,
                2,
                false,
                Default::default()
            )
        );
        for read in reads {
            read?;
        }
        pulse?;
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), parallel).await??;
    expire(&pool, node.node_id).await?;
    assert!(first.sync_node(&node, &key, connected.run).await.is_err());
    assert!(
        first
            .pulse_session(
                &node,
                &key,
                connected.run,
                connected.epoch,
                3,
                false,
                Default::default()
            )
            .await
            .is_err()
    );
    let recovered = first
        .open_session(
            &node,
            &key,
            Open {
                connection: Uuid::new_v4(),
                previous_epoch: connected.epoch,
                ..hello
            },
        )
        .await?;
    assert_ne!(recovered.run.fence, connected.run.fence);
    assert!(second.sync_node(&node, &key, connected.run).await.is_err());
    expire(&pool, node.node_id).await?;
    let rebooted = second.open_session(&node, &key, competing).await?;
    assert_ne!(rebooted.run.boot, recovered.run.boot);
    assert_ne!(rebooted.run.fence, recovered.run.fence);
    expire(&pool, node.node_id).await?;
    assert!(
        first
            .open_session(
                &node,
                &key,
                Open {
                    connection: Uuid::new_v4(),
                    ..hello
                }
            )
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO node_boots(node_id,credential_generation,boot_id,retired_at) SELECT $1,$2,uuidv7(),now() FROM generate_series(1,4094)")
        .bind(node.node_id).bind(node.credential_generation).execute(&pool).await?;
    let full = first
        .open_session(
            &node,
            &key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await;
    assert_eq!(
        crate::model::error(&full.err().unwrap()),
        Some(crate::model::Error::NodeLimit)
    );
    let rotated = first.rotate_node(&admin, "data-test", &key).await?;
    assert_eq!(rotated.node_id, node.node_id);
    assert_eq!(
        rotated.credential_generation,
        node.credential_generation + 1
    );
    assert_ne!(rotated.credential, node.credential);
    assert!(second.open_session(&node, &key, competing).await.is_err());
    let final_run = second.open_session(&rotated, &key, hello).await?;
    let draining = first
        .pulse_session(
            &rotated,
            &key,
            final_run.run,
            final_run.epoch,
            1,
            true,
            Default::default(),
        )
        .await?;
    assert!(draining.draining);
    assert!(
        second
            .pulse_session(
                &rotated,
                &key,
                final_run.run,
                final_run.epoch,
                2,
                false,
                Default::default()
            )
            .await?
            .draining
    );
    first.release("data-test").await?;
    assert!(
        second
            .sync_node(&rotated, &key, final_run.run)
            .await
            .is_err()
    );
    let restored = first.rotate_node(&admin, "data-test", &key).await?;
    assert_eq!(restored.node_id, node.node_id);
    assert!(second.open_session(&restored, &key, hello).await.is_ok());

    let reporting = first
        .provision_node(&admin, "reporting", "worker", &key)
        .await?;
    let opening = Open {
        boot: Uuid::new_v4(),
        connection: Uuid::new_v4(),
        previous_epoch: 0,
    };
    let report_run = first.open_session(&reporting, &key, opening).await?;
    let mut report = crate::model::node::NodeReport {
        boot_id: opening.boot,
        sequence: 1,
        sampled_at: chrono::Utc::now(),
        metadata: Some(serde_json::json!({"format":1,"revisions":{"requests":1}})),
        status: serde_json::json!({"work":"normal","runtime":{"started_at":chrono::Utc::now(),"uptime_seconds":10}}),
    };
    assert!(
        second
            .report_session(&reporting, &key, report_run.run, &report, 10)
            .await?
    );
    assert!(
        !first
            .report_session(&reporting, &key, report_run.run, &report, 10)
            .await?
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_runtime_samples WHERE node_id=$1")
            .bind(reporting.node_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 1);
    let reconnected = second.open_session(&reporting, &key, opening).await?;
    assert!(
        !second
            .report_session(&reporting, &key, report_run.run, &report, 10)
            .await?
    );
    report.sequence = 2;
    report.metadata = None;
    assert!(
        first
            .report_session(&reporting, &key, report_run.run, &report, 0)
            .await?
    );
    let saved = second.node_report(reporting.node_id).await?.unwrap();
    assert_eq!(saved.metadata["revisions"]["requests"], 1);
    expire(&pool, reporting.node_id).await?;
    let renewed = first
        .open_session(
            &reporting,
            &key,
            Open {
                connection: Uuid::new_v4(),
                previous_epoch: reconnected.epoch,
                ..opening
            },
        )
        .await?;
    report.sequence = 3;
    assert!(
        second
            .report_session(&reporting, &key, report_run.run, &report, 0)
            .await
            .is_err()
    );
    assert!(
        second
            .close_session(&reporting, &key, report_run.run)
            .await
            .is_err()
    );
    assert!(
        second
            .report_session(&reporting, &key, renewed.run, &report, 0)
            .await?
    );
    first.close_session(&reporting, &key, renewed.run).await?;
    second.close_session(&reporting, &key, renewed.run).await?;
    assert!(
        first
            .sync_node(&reporting, &key, renewed.run)
            .await
            .is_err()
    );
    assert!(
        first
            .open_session(
                &reporting,
                &key,
                Open {
                    connection: Uuid::new_v4(),
                    previous_epoch: renewed.epoch,
                    ..opening
                }
            )
            .await
            .is_err()
    );
    let rebooted = first
        .open_session(
            &reporting,
            &key,
            Open {
                boot: Uuid::new_v4(),
                connection: Uuid::new_v4(),
                previous_epoch: 0,
            },
        )
        .await?;
    assert!(
        first
            .close_session(&reporting, &key, renewed.run)
            .await
            .is_err()
    );
    assert!(
        first
            .report_session(&reporting, &key, renewed.run, &report, 0)
            .await
            .is_err()
    );
    assert!(
        second
            .sync_node(&reporting, &key, rebooted.run)
            .await
            .is_ok()
    );
    first.release("reporting").await?;
    report.boot_id = rebooted.run.boot;
    assert!(
        second
            .report_session(&reporting, &key, rebooted.run, &report, 0)
            .await
            .is_err()
    );
    assert!(
        second
            .close_session(&reporting, &key, rebooted.run)
            .await
            .is_err()
    );
    Ok(())
}
