use super::*;

pub(in super::super) async fn update_storage_profile(
    pool: &SqlitePool,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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
    async fn insert(&self, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        validate_block_key(tx, self.key).await?;
        queries::insert_storage_record(&self.record, self.key)
            .execute(&mut **tx)
            .await?;
        queries::insert_storage_profile(
            self.record.id,
            self.read,
            self.upload,
            self.control,
            self.record.r#type == "s3",
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
    write: SqlitePool,
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
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
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
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
        prepared.insert(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(in super::super) async fn storage_creation(
    read: &SqlitePool,
    write: &SqlitePool,
) -> Result<StorageCreation> {
    Ok(StorageCreation {
        deployment: queries::storage_deployment().fetch_one(read).await?,
        mode: "single".into(),
        writer: Box::new(StorageInitialization {
            write: write.clone(),
            attempted: false,
            prepared: None,
        }),
    })
}

struct StorageModification {
    original_type: String,
    write: SqlitePool,
    id: Uuid,
    revision: i64,
}

pub(in super::super) async fn storage_edit(
    read: &SqlitePool,
    write: &SqlitePool,
    name: &str,
) -> Result<StorageEdit> {
    let mut tx = read.begin().await?;
    let row = load_row(&mut tx, name).await?;
    let profile = queries::storage_profile_lock(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let id = row.id;
    let revision = profile.revision;
    tx.commit().await?;
    let original_type = row.r#type.clone();
    Ok(StorageEdit {
        row,
        profile,
        mode: "single".into(),
        update: Box::new(StorageModification {
            original_type,
            write: write.clone(),
            id,
            revision,
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
            record.id == self.id && profile.storage_id == self.id,
            "storage update belongs to another storage"
        );
        ensure!(
            profile.revision == self.revision,
            "storage update revision changed"
        );
        validate_storage_profile(profile, &record.r#type)?;
        profile.validate_key(key)?;
        let mut tx = self.write.begin_with(queries::BEGIN_WRITE).await?;
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
                    "shared_s3"
                },
            )
            .execute(&mut *tx)
            .await?;
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
    pool: &SqlitePool,
    name: &str,
    key: Option<&str>,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
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

pub(super) async fn administration_transaction(
    pool: &SqlitePool,
    principal: &Principal,
) -> Result<Transaction<'static, Sqlite>> {
    let mut tx = pool.begin_with(queries::BEGIN_WRITE).await?;
    if !queries::validate_identity(principal, &mut tx).await? {
        return Err(denied());
    }
    Ok(tx)
}

pub(super) async fn enable_member_mode(tx: &mut Transaction<'_, Sqlite>, role: &str) -> Result<()> {
    if role == "member" {
        queries::enable_project_mode().execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn protect_admin(
    tx: &mut Transaction<'_, Sqlite>,
    current: &User,
    role: &str,
    enabled: bool,
) -> Result<()> {
    if current.role == "admin" && current.enabled && (role != "admin" || !enabled) {
        let count: i64 = queries::admin_count().fetch_one(&mut **tx).await?;
        if count <= 1 {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    Ok(())
}
