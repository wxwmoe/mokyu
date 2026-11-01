use super::*;

async fn save_storage_profile(
    tx: &mut Transaction<'_, MySql>,
    profile: &StorageProfile,
) -> Result<()> {
    let affected = queries::save_storage_profile(profile)?
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if affected != 1 {
        return Err(crate::model::Error::StorageProfileConflict.into());
    }
    Ok(())
}

async fn validate_block_key(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let Some(key_id) = key_id else {
        return Ok(());
    };
    let valid = queries::valid_storage_key(key_id)
        .fetch_optional(&mut **tx)
        .await?;
    anyhow::ensure!(valid.is_some(), "active block key was not found");
    Ok(())
}

pub(super) async fn lock_mode(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> anyhow::Result<(Uuid, String)> {
    Ok((
        queries::storage_mode_lock().fetch_one(&mut **tx).await?,
        "single".into(),
    ))
}

async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    name: &str,
) -> anyhow::Result<crate::model::storage::StorageRow> {
    queries::storage_row_lock(name)
        .fetch_optional(&mut **tx)
        .await?
        .context("storage was not found")
}

pub(in super::super) async fn update_storage_profile(
    pool: &MySqlPool,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin().await?;
    let (storage_id, storage_type, key): (Uuid, String, Option<Uuid>) =
        queries::storage_profile_target(&name)
            .fetch_optional(&mut *tx)
            .await?
            .context("storage was not found")?;
    anyhow::ensure!(
        profile.storage_id == storage_id,
        "storage profile belongs to another storage"
    );
    validate_storage_profile(&profile, &storage_type)?;
    profile.validate_key(key)?;
    save_storage_profile(&mut tx, &profile).await?;
    checkpoint_event(
        &mut tx,
        "storage.profile.update",
        &name,
        serde_json::json!({"revision": profile.revision + 1}),
    )
    .await?;
    tx.commit().await?;
    Ok(storage_id)
}

struct StorageInitialization {
    tx: Option<Transaction<'static, MySql>>,
    id: Option<Uuid>,
}

#[async_trait::async_trait]
impl StorageInitializer for StorageInitialization {
    async fn prepare(
        &mut self,
        record: &BackendRecord,
        key: Option<Uuid>,
        read: Option<i32>,
        upload: Option<i32>,
        control: Option<i32>,
        profile: Option<StorageProfile>,
    ) -> Result<StorageProfile> {
        anyhow::ensure!(self.id.is_none(), "storage is already prepared");
        let mut tx = self.tx.take().context("storage preparation failed")?;
        validate_block_key(&mut tx, key).await?;
        queries::insert_storage_record(record, key)
            .execute(&mut *tx)
            .await?;
        queries::insert_storage_profile(record.id, read, upload, control, record.r#type == "s3")
            .execute(&mut *tx)
            .await?;
        if let Some(mut profile) = profile {
            profile.storage_id = record.id;
            profile.revision = 1;
            validate_storage_profile(&profile, &record.r#type)?;
            profile.validate_key(key)?;
            save_storage_profile(&mut tx, &profile).await?;
        }
        let effective = queries::storage_profile(record.id)
            .fetch_one(&mut *tx)
            .await?;
        validate_storage_profile(&effective, &record.r#type)?;
        self.tx = Some(tx);
        self.id = Some(record.id);
        Ok(effective)
    }
    async fn finish(mut self: Box<Self>) -> Result<()> {
        self.id.context("storage was not prepared")?;
        self.tx
            .take()
            .context("storage preparation failed")?
            .commit()
            .await?;
        Ok(())
    }
}

pub(in super::super) async fn storage_creation(pool: &MySqlPool) -> Result<StorageCreation> {
    let mut tx = pool.begin().await?;
    let (deployment, mode) = lock_mode(&mut tx).await?;
    Ok(StorageCreation {
        deployment,
        mode,
        writer: Box::new(StorageInitialization {
            tx: Some(tx),
            id: None,
        }),
    })
}

struct StorageModification {
    original_type: String,
    tx: Transaction<'static, MySql>,
    id: Uuid,
}

pub(in super::super) async fn storage_edit(pool: &MySqlPool, name: &str) -> Result<StorageEdit> {
    let mut tx = pool.begin().await?;
    let (_, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, name).await?;
    let profile: StorageProfile = queries::storage_profile_lock(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let id = row.id;
    let original_type = row.r#type.clone();
    Ok(StorageEdit {
        row,
        profile,
        mode,
        update: Box::new(StorageModification {
            tx,
            id,
            original_type,
        }),
    })
}

#[async_trait::async_trait]
impl StorageUpdate for StorageModification {
    async fn finish(
        mut self: Box<Self>,
        record: &BackendRecord,
        key: Option<Uuid>,
        profile: &StorageProfile,
    ) -> Result<()> {
        ensure!(
            record.id == self.id && profile.storage_id == self.id,
            "storage update belongs to another storage"
        );
        validate_storage_profile(profile, &record.r#type)?;
        profile.validate_key(key)?;
        ensure!(
            !super::super::queries::redundancy::relocation(record)
                .fetch_one(&mut *self.tx)
                .await?,
            s3s::s3_error!(
                OperationAborted,
                "storage still has redundancy or tiering dependencies"
            )
        );
        if self.original_type != record.r#type {
            ensure!(
                queries::storage_relocation_ready()
                    .fetch_one(&mut *self.tx)
                    .await?,
                crate::model::Error::StorageNotDrained
            );
            queries::relocate_raw_sources(
                record.id,
                if record.r#type == "local" {
                    "local"
                } else {
                    "shared_s3"
                },
            )
            .execute(&mut *self.tx)
            .await?;
        }
        validate_block_key(&mut self.tx, key).await?;
        queries::update_storage_record(record, key)
            .execute(&mut *self.tx)
            .await?;
        save_storage_profile(&mut self.tx, profile).await?;
        self.tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn storage_current_key(
    pool: &MySqlPool,
    name: &str,
    key: Option<&str>,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin().await?;
    lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, name).await?;
    let key_id = if let Some(key) = key {
        let parsed = key.parse::<Uuid>().ok();
        queries::current_storage_key(parsed, key.trim())
            .fetch_optional(&mut *tx)
            .await?
            .context("active block key was not found")
            .map(Some)?
    } else {
        None
    };
    if key_id.is_some() {
        queries::storage_profile_lock(row.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(key_id)?;
    }
    queries::update_storage_key(row.id, key_id)
        .execute(&mut *tx)
        .await?;
    queries::bump_storage_profile(row.id)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "storage.current_key.update",
        name,
        serde_json::json!({"block_key_id":key_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(row.id)
}

pub(in super::super) async fn require_sources(
    db: &mut sqlx::MySqlConnection,
    stream: Option<Id>,
    chunks: &[Id],
) -> Result<()> {
    use futures_util::TryStreamExt;
    // Lock all logical rows before any physical row; shared validation locks block quarantine.
    for phase in 0..3 {
        let mut query = queries::source_locks(stream, chunks, phase);
        let mut rows = query.build_query_scalar::<i64>().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    ensure!(
        !queries::unavailable_sources(stream, chunks)
            .build_query_scalar::<bool>()
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}
