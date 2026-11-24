use super::{queries, storage_payload};
use crate::model::storage_cluster::Manifest;
use anyhow::Result;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{path::Path, sync::Arc};

pub(crate) struct OfflineCatalog {
    pool: SqlitePool,
    readonly: bool,
}
type Backend = OfflineCatalog;

mod recovery {
    include!("../../shared/storage_recovery.rs");
}

async fn begin_recovery(backend: &Backend) -> Result<queries::storage_recovery::Tx<'_>> {
    Ok(if backend.readonly {
        backend.pool.begin().await?
    } else {
        backend.pool.begin_with("BEGIN IMMEDIATE").await?
    })
}

impl OfflineCatalog {
    pub async fn open(path: &Path, readonly: bool) -> Result<Arc<Self>> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .read_only(readonly)
            .create_if_missing(!readonly);
        let pool = SqlitePoolOptions::new()
            .max_connections(if readonly { 4 } else { 1 })
            .connect_with(options)
            .await?;
        if !readonly {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='storage_recovery_catalogs')",
            )
            .fetch_one(&pool)
            .await?;
            if !exists {
                let mut tx = pool.begin().await?;
                sqlx::raw_sql(include_str!("offline.sql"))
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
        }
        Ok(Arc::new(Self { pool, readonly }))
    }

    pub async fn copy_evidence(&self, source: uuid::Uuid, target: uuid::Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO storage_recovery_catalogs(id,namespace_id,anchor) SELECT ?,namespace_id,anchor FROM storage_recovery_catalogs WHERE id=?")
            .bind(target).bind(source).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO storage_recovery_entries SELECT ?,category,owner_id,id,position,content_hash,encoded_bytes,content FROM storage_recovery_entries WHERE catalog=? AND category<=2")
            .bind(target).bind(source).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn forget(&self, id: uuid::Uuid) -> Result<()> {
        sqlx::query("DELETE FROM storage_recovery_catalogs WHERE id=?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn layout(&self, manifest: &Manifest, current: bool) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO storage_cluster_history VALUES(?,?,?) ON CONFLICT DO NOTHING")
            .bind(manifest.cluster_id)
            .bind(manifest.revision)
            .bind(sqlx::types::Json(manifest))
            .execute(&mut *tx)
            .await?;
        if current {
            sqlx::query("INSERT INTO storage_clusters VALUES(?,?) ON CONFLICT DO NOTHING")
                .bind(manifest.cluster_id)
                .bind(sqlx::types::Json(manifest))
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
