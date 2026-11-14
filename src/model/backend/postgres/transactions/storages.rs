use super::*;

async fn save_storage_profile(
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let Some(key_id) = key_id else {
        return Ok(());
    };
    let valid: bool = queries::valid_storage_key(key_id)
        .fetch_one(&mut **tx)
        .await?;
    anyhow::ensure!(valid, "active block key was not found");
    Ok(())
}

pub(super) async fn lock_mode(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<(Uuid, String)> {
    Ok(queries::storage_mode_lock().fetch_one(&mut **tx).await?)
}

async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
) -> anyhow::Result<crate::model::storage::StorageRow> {
    Ok(queries::storage_row_lock(name)
        .fetch_optional(&mut **tx)
        .await?
        .context("storage was not found")?)
}

pub(in super::super) async fn update_storage_profile(
    pool: &PgPool,
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

struct PreparedStorage {
    record: BackendRecord,
    key: Option<Uuid>,
    read: Option<i32>,
    upload: Option<i32>,
    control: Option<i32>,
    profile: Option<StorageProfile>,
}

impl PreparedStorage {
    async fn insert(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        let (_, mode) = lock_mode(tx).await?;
        self.record.validate_mode(&mode)?;
        validate_block_key(tx, self.key).await?;
        queries::insert_storage_record(&self.record, self.key)
            .execute(&mut **tx)
            .await?;
        queries::insert_storage_profile(
            self.record.id,
            self.read,
            self.upload,
            self.control,
            self.record.r#type != "local",
        )
        .execute(&mut **tx)
        .await?;
        if let Some(profile) = &self.profile {
            save_storage_profile(tx, profile).await?;
        }
        Ok(())
    }
}

struct StorageInitialization {
    pool: PgPool,
    attempted: bool,
    prepared: Option<PreparedStorage>,
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
        mut profile: Option<StorageProfile>,
    ) -> Result<StorageProfile> {
        ensure!(!self.attempted, "storage preparation was already attempted");
        self.attempted = true;
        if let Some(profile) = &mut profile {
            profile.storage_id = record.id;
            profile.revision = 1;
            validate_storage_profile(profile, &record.r#type)?;
            profile.validate_key(key)?;
        }
        let prepared = PreparedStorage {
            record: record.clone(),
            key,
            read,
            upload,
            control,
            profile,
        };
        // Validate native constraints, then release the writer before external storage I/O.
        let mut tx = self.pool.begin().await?;
        prepared.insert(&mut tx).await?;
        let effective = queries::storage_profile(record.id)
            .fetch_one(&mut *tx)
            .await?;
        validate_storage_profile(&effective, &record.r#type)?;
        tx.rollback().await?;
        self.prepared = Some(prepared);
        Ok(effective)
    }
    async fn finish(mut self: Box<Self>) -> Result<()> {
        let prepared = self.prepared.take().context("storage was not prepared")?;
        let mut tx = self.pool.begin().await?;
        prepared.insert(&mut tx).await?;
        super::super::storage_native::initialized(&mut tx, &prepared.record).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn storage_creation(pool: &PgPool) -> Result<StorageCreation> {
    let mut tx = pool.begin().await?;
    let (deployment, mode) = lock_mode(&mut tx).await?;
    tx.rollback().await?;
    Ok(StorageCreation {
        deployment,
        mode,
        writer: Box::new(StorageInitialization {
            pool: pool.clone(),
            attempted: false,
            prepared: None,
        }),
    })
}

struct StorageModification {
    original_type: String,
    pool: PgPool,
    id: Uuid,
    revision: i64,
    mode: String,
}

pub(in super::super) async fn storage_edit(pool: &PgPool, name: &str) -> Result<StorageEdit> {
    let mut tx = pool.begin().await?;
    let (_, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, name).await?;
    let profile: StorageProfile = queries::storage_profile_lock(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let original_type = row.r#type.clone();
    let id = row.id;
    let revision = profile.revision;
    tx.commit().await?;
    Ok(StorageEdit {
        row,
        profile,
        mode: mode.clone(),
        update: Box::new(StorageModification {
            pool: pool.clone(),
            id,
            revision,
            mode,
            original_type,
        }),
    })
}

#[async_trait::async_trait]
impl StorageUpdate for StorageModification {
    async fn finish(
        self: Box<Self>,
        record: &BackendRecord,
        key: Option<Uuid>,
        profile: &StorageProfile,
    ) -> Result<()> {
        ensure!(
            record.id == self.id
                && profile.storage_id == self.id
                && profile.revision == self.revision,
            "storage update changed its identity or revision"
        );
        validate_storage_profile(profile, &record.r#type)?;
        profile.validate_key(key)?;
        let mut tx = self.pool.begin().await?;
        let (_, mode) = lock_mode(&mut tx).await?;
        ensure!(
            mode == self.mode,
            crate::model::runtime::ModeConflict("deployment mode changed while checking storage")
        );
        let current = load_row(&mut tx, &record.name).await?;
        let current_profile = queries::storage_profile_lock(self.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            current.id == self.id
                && current.r#type == self.original_type
                && current_profile.revision == self.revision,
            crate::model::Error::StorageProfileConflict
        );
        ensure!(
            !super::super::queries::redundancy::relocation(record)
                .fetch_one(&mut *tx)
                .await?,
            s3s::s3_error!(
                OperationAborted,
                "storage still has redundancy or tiering dependencies"
            )
        );
        if self.original_type != record.r#type {
            ensure!(
                queries::storage_relocation_ready()
                    .fetch_one(&mut *tx)
                    .await?,
                crate::model::Error::StorageNotDrained
            );
            queries::relocate_raw_sources(
                record.id,
                if record.r#type == "local" {
                    "local"
                } else {
                    "shared"
                },
            )
            .execute(&mut *tx)
            .await?;
        }
        if !record.enabled || record.r#type == "local" {
            let mode = queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
            if mode != "single" {
                ensure!(
                    record.r#type != "local",
                    crate::model::runtime::ModeConflict("local storage requires single mode")
                );
                ensure!(
                    queries::cluster_staging_id()
                        .fetch_optional(&mut *tx)
                        .await?
                        .flatten()
                        != Some(record.id),
                    crate::model::runtime::ModeConflict(
                        "select another shared staging storage before disabling it"
                    )
                );
            }
        }
        validate_block_key(&mut tx, key).await?;
        queries::update_storage_record(record, key)
            .execute(&mut *tx)
            .await?;
        save_storage_profile(&mut tx, profile).await?;

        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn storage_current_key(
    pool: &PgPool,
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
