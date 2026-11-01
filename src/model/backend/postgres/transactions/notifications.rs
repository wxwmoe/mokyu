use super::super::queries::notifications as query;
use crate::model::notification::{Change, Consumer, Delivery, LIMIT};
use anyhow::{Result, ensure};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub(crate) async fn activate(db: &mut PgConnection, consumer: Consumer, role: &str) -> Result<()> {
    // Identity/session rows are already locked; publication only reads consumers.
    query::membership().execute(&mut *db).await?;
    query::activate(consumer, role).execute(&mut *db).await?;
    query::retire_previous(consumer).execute(db).await?;
    Ok(())
}

pub(crate) async fn claim(pool: &PgPool, consumers: &[Consumer]) -> Result<Vec<Delivery>> {
    ensure!(consumers.len() <= 4096, "too many notification consumers");
    if consumers.is_empty() {
        return Ok(Vec::new());
    }
    let nodes: Vec<_> = consumers.iter().map(|value| value.node).collect();
    let generations: Vec<_> = consumers.iter().map(|value| value.generation).collect();
    let rows = query::claim(&nodes, &generations, Uuid::new_v4())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|(node, generation, id, scope, resource, key)| {
            let change = match scope.as_str() {
                "object" => Change::Object {
                    bucket: resource,
                    key,
                },
                "bucket" => Change::Bucket { bucket: resource },
                "storage" => Change::Storage { storage: resource },
                _ => anyhow::bail!("invalid notification scope"),
            };
            change.validate()?;
            Ok(Delivery {
                consumer: Consumer { node, generation },
                id,
                change,
            })
        })
        .collect()
}

pub(crate) async fn acknowledge(pool: &PgPool, consumer: Consumer, ids: &[Uuid]) -> Result<u64> {
    ensure!(
        ids.len() <= LIMIT && ids.iter().all(|id| !id.is_nil()),
        "invalid notification receipt"
    );
    Ok(query::acknowledge(consumer, ids)
        .execute(pool)
        .await?
        .rows_affected())
}

pub(crate) async fn maintain(pool: &PgPool) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let nodes = query::expired_nodes().fetch_all(&mut *tx).await?;
    query::retire_expired(&nodes).execute(&mut *tx).await?;
    let mut more = false;
    for _ in 0..16 {
        more = query::collect().execute(&mut *tx).await?.rows_affected() == 512;
        if !more {
            break;
        }
    }
    tx.commit().await?;
    Ok(more || nodes.len() == 32)
}
