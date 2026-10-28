use super::{migration, queries};
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a new isolated PostgreSQL database"]
async fn postgres_maintenance_state_tracks_config_and_due_reuse() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let url = std::fs::read_to_string(std::env::var("MOKYU_TEST_DATABASE_URL_FILE")?)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.trim())
        .await?;
    assert!(queries::metadata_table().fetch_one(&pool).await?.is_none());
    migration::run(&mut *pool.acquire().await?).await?;
    sqlx::query(
        "UPDATE mokyu_meta SET maintenance=false,pack_creation_paused=false,gc_paused=false",
    )
    .execute(&pool)
    .await?;
    let first = queries::maintenance_state().fetch_one(&pool).await?;
    let second = queries::maintenance_state().fetch_one(&pool).await?;
    assert_eq!(first.0, second.0);
    assert!(!second.1);
    assert!(second.2 >= first.2);

    let storage: Uuid = sqlx::query_scalar("INSERT INTO storages(name,type,path) VALUES('schedule','local','/schedule-fixture') RETURNING id")
        .fetch_one(&pool).await?;
    sqlx::query("INSERT INTO storage_profiles(storage_id) VALUES($1)")
        .bind(storage)
        .execute(&pool)
        .await?;
    let configured = queries::maintenance_state().fetch_one(&pool).await?;
    assert_ne!(configured.0, first.0);
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=$1")
        .bind(storage)
        .execute(&mut *tx)
        .await?;
    assert_eq!(
        queries::maintenance_state().fetch_one(&pool).await?.0,
        configured.0
    );
    tx.rollback().await?;
    assert_eq!(
        queries::maintenance_state().fetch_one(&pool).await?.0,
        configured.0
    );
    sqlx::query("UPDATE storage_profiles SET revision=revision+1 WHERE storage_id=$1")
        .bind(storage)
        .execute(&pool)
        .await?;
    assert_ne!(
        queries::maintenance_state().fetch_one(&pool).await?.0,
        configured.0
    );

    let pack: Uuid = sqlx::query_scalar("INSERT INTO packs(storage_id,encryption_algorithm,raw_size,member_count,state,pack_fingerprint) VALUES($1,'none',2,2,'preparing',$2) RETURNING id")
        .bind(storage).bind(vec![0u8; 32]).fetch_one(&pool).await?;
    sqlx::query("INSERT INTO pack_changes(pack_id,next_check_at) VALUES($1,clock_timestamp()+interval '1 hour')")
        .bind(pack).execute(&pool).await?;
    let waiting = queries::maintenance_state().fetch_one(&pool).await?;
    assert!(!waiting.1);
    sqlx::query("UPDATE pack_changes SET next_check_at=clock_timestamp()-interval '1 second'")
        .execute(&pool)
        .await?;
    let due = queries::maintenance_state().fetch_one(&pool).await?;
    assert_eq!(due.0, waiting.0);
    assert!(due.1);
    for statement in [
        "UPDATE maintenance_controls SET paused=true WHERE policy='reuse'",
        "UPDATE mokyu_meta SET maintenance=true",
        "UPDATE storages SET enabled=false",
    ] {
        sqlx::raw_sql("UPDATE maintenance_controls SET paused=false; UPDATE mokyu_meta SET maintenance=false; UPDATE storages SET enabled=true")
            .execute(&pool).await?;
        sqlx::query(statement).execute(&pool).await?;
        let paused = queries::maintenance_state().fetch_one(&pool).await?;
        assert!(!paused.1);
        assert_ne!(paused.0, due.0);
    }
    let before = queries::maintenance_state().fetch_one(&pool).await?;
    sqlx::query("UPDATE maintenance_controls SET last_scheduled_at=clock_timestamp() WHERE policy='cleanup'")
        .execute(&pool).await?;
    assert_ne!(
        queries::maintenance_state().fetch_one(&pool).await?.0,
        before.0
    );
    Ok(())
}
