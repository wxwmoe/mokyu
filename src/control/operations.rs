use crate::{
    app::{App, Bucket},
    authorization::{Action, Principal},
    cli::{
        Buckets, Cache, Cleanup, Cluster, Command, ControlSettings, Credentials, Domains, Gc,
        Integrity, Keys, Maintenance, Packs, Projects, RewriteCommand, Settings, Setup, StorageKey,
        StorageProfiles, Storages, Tasks, Users,
    },
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::sync::Arc;
static PASSWORD_JOBS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
pub async fn password_matches(hash: String, password: String) -> Result<bool> {
    let job = PASSWORD_JOBS
        .try_acquire()
        .map_err(|_| s3s::s3_error!(SlowDown))?;
    Ok(tokio::task::spawn_blocking(move || {
        let _job = job;
        use argon2::{Argon2, PasswordHash, PasswordVerifier};
        PasswordHash::new(&hash).ok().is_some_and(|hash| {
            Argon2::default()
                .verify_password(password.as_bytes(), &hash)
                .is_ok()
        })
    })
    .await?)
}
pub async fn password_hash(password: String) -> Result<String> {
    ensure!(
        password.len() >= 12 && password.len() <= 1024,
        "password must contain 12..1024 bytes"
    );
    let job = PASSWORD_JOBS
        .try_acquire()
        .map_err(|_| s3s::s3_error!(SlowDown))?;
    tokio::task::spawn_blocking(move || {
        let _job = job;
        use argon2::{
            Argon2,
            password_hash::{PasswordHasher, SaltString},
        };
        let mut random = [0; 16];
        aws_lc_rs::rand::fill(&mut random)
            .map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
        let salt =
            SaltString::encode_b64(&random).map_err(|_| anyhow::anyhow!("salt encoding failed"))?;
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|s| s.to_string())
            .map_err(|_| anyhow::anyhow!("password hashing failed"))
    })
    .await?
}
pub async fn execute_as(app: &Arc<App>, principal: &Principal, command: Command) -> Result<Value> {
    if !super::audit::active()
        && let Some(action) = command.audit_action()
    {
        let id = super::audit::begin(&app.db, principal, action, None).await?;
        let result = super::audit::scope(id, execute_inner(app, principal, command)).await;
        super::audit::finish(&app.db, id, if result.is_ok() { 200 } else { 400 }, false).await;
        return result;
    }
    execute_inner(app, principal, command).await
}
async fn execute_inner(app: &Arc<App>, principal: &Principal, command: Command) -> Result<Value> {
    match command {
        Command::Init {
            username,
            password,
            require_change,
            ..
        } => {
            let local_control = matches!(principal, Principal::Bootstrap)
                || matches!(principal, Principal::Node { role, .. } if role == "control");
            ensure!(local_control, "init requires a local control identity");
            let id = super::account::initialize_admin(
                app,
                username,
                password.context("missing administrator password")?,
                require_change,
            )
            .await?;
            Ok(json!({"id": id}))
        }
        Command::Quota(command) => {
            use crate::cli::Quotas;
            let result = match command {
                Quotas::Show { scope, id } => {
                    super::quotas::read(app, principal, &scope, id).await?
                }
                Quotas::Set {
                    scope,
                    id,
                    document,
                    ..
                } => {
                    super::quotas::save(
                        app,
                        principal,
                        &scope,
                        id,
                        serde_json::from_value(document.context("missing quota document")?)?,
                    )
                    .await?
                }
            };
            Ok(serde_json::to_value(result)?)
        }
        Command::Audit {
            after,
            actor,
            action,
        } => Ok(serde_json::to_value(
            super::audit::page(
                app,
                true,
                None,
                super::audit::Filter {
                    after,
                    actor,
                    action,
                    ..Default::default()
                },
                false,
            )
            .await?,
        )?),
        Command::Project(project) => {
            use super::projects::{self, ProjectInput};
            match project {
                Projects::List => Ok(serde_json::to_value(projects::list_all(&app.db).await?)?),
                Projects::Create { name } => Ok(serde_json::to_value(
                    projects::save(
                        app,
                        principal,
                        None,
                        ProjectInput {
                            name,
                            description: String::new(),
                            allow_bucket_create: false,
                        },
                    )
                    .await?,
                )?),
                Projects::Update {
                    id,
                    name,
                    description,
                    allow_bucket_create,
                } => Ok(serde_json::to_value(
                    projects::save(
                        app,
                        principal,
                        Some(id),
                        ProjectInput {
                            name,
                            description,
                            allow_bucket_create,
                        },
                    )
                    .await?,
                )?),
                Projects::Delete { id } => {
                    projects::remove(app, principal, id).await?;
                    Ok(json!({"deleted": id}))
                }
                Projects::Mode { enabled } => {
                    projects::mode(app, principal, enabled).await?;
                    Ok(json!({"enabled": enabled}))
                }
            }
        }
        Command::Status { ready } => {
            let status = app.status().await?;
            if ready {
                sqlx::query("SELECT 1").execute(&app.db).await?;
                ensure!(!app.mode_mismatch().await?, "service is in mode_mismatch");
                ensure!(!app.recovery_required(), "node recovery is required");
                ensure!(
                    app.role == "single" || app.node.get().is_some(),
                    "node is not registered for the current deployment mode"
                );
            }
            Ok(status)
        }
        Command::Cluster(command) => match command {
            Cluster::Status => {
                let mode: String =
                    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
                        .fetch_one(&app.db)
                        .await?;
                let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
                    .fetch_one(&app.db)
                    .await?;
                let local_storages: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM storages WHERE type='local'")
                        .fetch_one(&app.db)
                        .await?;
                Ok(json!({"mode": mode, "enabled_nodes": nodes, "local_storages": local_storages}))
            }
            Cluster::Mode { mode } => {
                let mut tx = app.db.begin().await?;
                let current: String = sqlx::query_scalar(
                    "SELECT deployment_mode FROM mokyu_meta WHERE singleton FOR UPDATE",
                )
                .fetch_one(&mut *tx)
                .await?;
                if current == mode {
                    tx.rollback().await?;
                    return Ok(json!({"mode": mode}));
                }
                if mode == "cluster" {
                    let local: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM storages WHERE type='local')",
                    )
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(
                        !local,
                        "local storages exist; migrate them to S3 and update their storage before enabling cluster mode"
                    );
                    let staging: Option<uuid::Uuid> = sqlx::query_scalar(
                        "SELECT NULLIF(value->>'staging_storage_id','')::uuid FROM runtime_settings WHERE section='uploads'",
                    )
                    .fetch_one(&mut *tx)
                    .await?;
                    let valid_staging: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM storages WHERE id=$1 AND enabled AND type='s3')",
                    )
                    .bind(staging)
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(
                        valid_staging,
                        "cluster mode requires an enabled S3 uploads.staging_storage_id"
                    );
                } else {
                    let local_node = match principal {
                        Principal::Node { id, role } if role == "control" => Some(*id),
                        _ => None,
                    };
                    let other_nodes: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled AND ($1::uuid IS NULL OR id<>$1))",
                    )
                    .bind(local_node)
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(
                        !other_nodes,
                        "disable all registered cluster nodes before returning to single mode"
                    );
                    let pending: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM pending_uploads) OR EXISTS(SELECT 1 FROM fragments WHERE staging_storage_id IS NOT NULL)",
                    )
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(
                        !pending,
                        "complete or cancel active uploads before returning to single mode"
                    );
                }
                if mode == "single"
                    && let Principal::Node { id, role } = principal
                    && role == "control"
                {
                    sqlx::query("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE id=$1")
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'")
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1")
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1,deployment_mode_revision=deployment_mode_revision+1 WHERE singleton")
                    .bind(&mode)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                Ok(json!({"mode": mode}))
            }
            Cluster::Nodes => {
                let rows: Vec<(
                    uuid::Uuid,
                    String,
                    String,
                    bool,
                    Option<chrono::DateTime<chrono::Utc>>,
                    chrono::DateTime<chrono::Utc>,
                )> = sqlx::query_as(
                    "SELECT id,name,role,enabled,last_seen_at,created_at FROM nodes ORDER BY name",
                )
                .fetch_all(&app.db)
                .await?;
                Ok(json!(rows
                    .into_iter()
                    .map(|(id, name, kind, enabled, last_seen_at, created_at)| {
                        json!({"id":id,"name":name,"role":kind,"enabled":enabled,"last_seen_at":last_seen_at,"created_at":created_at})
                    })
                    .collect::<Vec<_>>()))
            }
            Cluster::Release { name, confirm } => {
                if matches!(principal, Principal::Bootstrap) {
                    let local_name = if app.role == "single" {
                        "single"
                    } else {
                        app.config.instance.name.as_deref().unwrap_or("")
                    };
                    ensure!(
                        name == local_name,
                        "bootstrap release may target only the local node"
                    );
                }
                ensure!(confirm, "cluster release requires --confirm");
                let id: uuid::Uuid = sqlx::query_scalar("UPDATE nodes SET enabled=false,credential_encrypted=NULL,last_seen_at=NULL,status='released',recovery_required=false,updated_at=now() WHERE name=$1 AND enabled RETURNING id")
                    .bind(&name).fetch_optional(&app.db).await?.ok_or_else(|| anyhow::anyhow!("node not found or already released"))?;
                sqlx::query("UPDATE tasks SET state=CASE WHEN execution_scope='node' THEN 'paused' ELSE 'queued' END,lease_node_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE lease_node_id=$1 AND state='running'")
                    .bind(id).execute(&app.db).await?;
                sqlx::query("DELETE FROM storage_permits WHERE holder_id=$1")
                    .bind(id)
                    .execute(&app.db)
                    .await?;
                Ok(json!({"released": name}))
            }
        },
        Command::Setup(Setup::Token) => {
            Ok(json!({"token": super::account::setup_token(app).await?}))
        }
        Command::Storages(command) => {
            type Row = (
                uuid::Uuid,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                String,
                Option<uuid::Uuid>,
                i64,
                Option<i32>,
                Option<i32>,
                Option<i32>,
                bool,
                chrono::DateTime<chrono::Utc>,
            );
            let public = |row: Row| {
                json!({
                    "id": row.0,
                    "name": row.1,
                    "type": row.2,
                    "path": row.3,
                    "endpoint": row.4,
                    "bucket": row.5,
                    "region": row.6,
                    "prefix": row.7,
                    "block_key_id": row.8,
                    "profile_revision": row.9,
                    "read_concurrency": row.10,
                    "upload_concurrency": row.11,
                    "control_concurrency": row.12,
                    "enabled": row.13,
                    "updated_at": row.14,
                })
            };
            match command {
                Storages::List => {
                    let rows: Vec<Row> = sqlx::query_as("SELECT s.id,s.name,s.type,s.path,s.endpoint_url,s.remote_bucket_name,s.region,s.prefix,s.block_key_id,p.revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id ORDER BY s.name")
                        .fetch_all(&app.db)
                        .await?;
                    Ok(json!(rows.into_iter().map(public).collect::<Vec<_>>()))
                }
                Storages::Check { name } => {
                    let row: Row = sqlx::query_as("SELECT s.id,s.name,s.type,s.path,s.endpoint_url,s.remote_bucket_name,s.region,s.prefix,s.block_key_id,p.revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=$1")
                        .bind(&name)
                        .fetch_optional(&app.db)
                        .await?
                        .context("storage not found")?;
                    app.storage.refresh_store(row.0).await?;
                    ensure!(row.13, "storage is disabled");
                    app.storage.check_storage(row.0, false).await?;
                    Ok(public(row))
                }
                Storages::Show { name } => {
                    let row: Row = sqlx::query_as("SELECT s.id,s.name,s.type,s.path,s.endpoint_url,s.remote_bucket_name,s.region,s.prefix,s.block_key_id,p.revision,p.read_concurrency,p.upload_concurrency,p.control_concurrency,s.enabled,s.updated_at FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=$1")
                        .bind(&name)
                        .fetch_optional(&app.db)
                        .await?
                        .context("storage not found")?;
                    Ok(public(row))
                }
                Storages::Key { command } => match command {
                    StorageKey::Set { storage, key } => Ok(serde_json::to_value(
                        super::storage::set_current_key(app, storage, Some(key)).await?,
                    )?),
                    StorageKey::Clear { storage } => Ok(serde_json::to_value(
                        super::storage::set_current_key(app, storage, None).await?,
                    )?),
                },
                Storages::Profile { command } => match command {
                    StorageProfiles::Show { name } => {
                        let storage_id: uuid::Uuid =
                            sqlx::query_scalar("SELECT id FROM storages WHERE name=$1")
                                .bind(&name)
                                .fetch_optional(&app.db)
                                .await?
                                .context("storage not found")?;
                        Ok(serde_json::to_value(
                            crate::storage::profile::StorageProfile::load(&app.db, storage_id)
                                .await?,
                        )?)
                    }
                    StorageProfiles::Set { name, document, .. } => {
                        let profile: crate::storage::profile::StorageProfile =
                            serde_json::from_value(
                                document.context("missing storage profile document")?,
                            )?;
                        Ok(serde_json::to_value(
                            super::storage::update_profile(&app, name, profile).await?,
                        )?)
                    }
                },
                Storages::Rewrite { storage, command } => {
                    let storage_id: uuid::Uuid =
                        sqlx::query_scalar("SELECT id FROM storages WHERE name=$1 OR id=$2::uuid")
                            .bind(&storage)
                            .bind(storage.parse::<uuid::Uuid>().ok())
                            .fetch_optional(&app.db)
                            .await?
                            .context("storage not found")?;
                    match command {
                        RewriteCommand::Preview { target_key } => {
                            app.rewrite_start(storage_id, target_key.as_deref(), false, None).await
                        }
                        RewriteCommand::Start { target_key, confirm } => {
                            app.rewrite_start(storage_id, target_key.as_deref(), true, Some(&confirm)).await
                        }
                        RewriteCommand::Status => Ok(sqlx::query_scalar::<_, serde_json::Value>(
                            "SELECT to_jsonb(t) FROM tasks t WHERE task_type='rewrite' AND detail->>'role'='parent' AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1",
                        )
                        .bind(storage_id.to_string())
                        .fetch_optional(&app.db)
                        .await?
                        .context("rewrite task not found")?),
                        RewriteCommand::Pause | RewriteCommand::Resume | RewriteCommand::Retry => {
                            let task: uuid::Uuid = sqlx::query_scalar(
                                "SELECT id FROM tasks WHERE task_type='rewrite' AND detail->>'role'='parent' AND detail->>'storage_id'=$1 ORDER BY created_at DESC LIMIT 1",
                            )
                            .bind(storage_id.to_string())
                            .fetch_optional(&app.db)
                            .await?
                            .context("rewrite task not found")?;
                            let resume = matches!(command, RewriteCommand::Resume | RewriteCommand::Retry);
                            app.task_change(task, resume).await
                        }
                    }
                }
                Storages::Create {
                    name,
                    r#type,
                    initialize,
                    key,
                    path,
                    endpoint,
                    bucket,
                    region,
                    prefix,
                    access_key,
                    secret_key,
                    read_concurrency,
                    upload_concurrency,
                    control_concurrency,
                } => {
                    let block_key_id = if let Some(key) = key {
                        sqlx::query_scalar::<_, uuid::Uuid>(
                            "SELECT id FROM managed_keys WHERE purpose='block' AND revoked_at IS NULL AND (name=$1 OR id=$2)",
                        )
                        .bind(&key)
                        .bind(key.parse::<uuid::Uuid>().ok())
                        .fetch_optional(&app.db)
                        .await?
                        .map(Some)
                        .context("active block key not found")?
                    } else {
                        None
                    };
                    let row = super::storage::create_record(
                        app,
                        super::storage::StorageInput {
                            profile: None,
                            name,
                            storage_type: r#type,
                            path: path.map(|value| value.to_string_lossy().into_owned()),
                            endpoint,
                            bucket,
                            region,
                            prefix,
                            block_key_id,
                            initialize,
                            access_key,
                            secret_key,
                            read_concurrency,
                            upload_concurrency,
                            control_concurrency,
                        },
                    )
                    .await?;
                    Ok(serde_json::to_value(row)?)
                }
                Storages::Update {
                    name,
                    r#type,
                    key,
                    path,
                    endpoint,
                    bucket,
                    region,
                    prefix,
                    access_key,
                    secret_key,
                    enabled,
                    read_concurrency,
                    clear_read_concurrency,
                    upload_concurrency,
                    clear_upload_concurrency,
                    control_concurrency,
                    clear_control_concurrency,
                } => {
                    let block_key_id = if let Some(key) = key {
                        Some(
                            sqlx::query_scalar::<_, uuid::Uuid>(
                                "SELECT id FROM managed_keys WHERE purpose='block' AND revoked_at IS NULL AND (name=$1 OR id=$2)",
                            )
                            .bind(&key)
                            .bind(key.parse::<uuid::Uuid>().ok())
                            .fetch_optional(&app.db)
                            .await?
                            .context("active block key not found")?,
                        )
                    } else {
                        None
                    };
                    let row = super::storage::update_record(
                        app,
                        name,
                        super::storage::StoragePatch {
                            profile: None,
                            storage_type: r#type,
                            path: path.map(|value| value.to_string_lossy().into_owned()),
                            endpoint,
                            bucket,
                            region,
                            prefix,
                            block_key_id: block_key_id.map(Some),
                            access_key,
                            secret_key,
                            enabled,
                            read_concurrency: if clear_read_concurrency {
                                Some(None)
                            } else {
                                read_concurrency.map(Some)
                            },
                            upload_concurrency: if clear_upload_concurrency {
                                Some(None)
                            } else {
                                upload_concurrency.map(Some)
                            },
                            control_concurrency: if clear_control_concurrency {
                                Some(None)
                            } else {
                                control_concurrency.map(Some)
                            },
                        },
                    )
                    .await?;
                    Ok(serde_json::to_value(row)?)
                }
                Storages::Sweep {
                    storage,
                    execute,
                    preview,
                    confirm_prefix,
                    older_than,
                } => {
                    app.sweep_start(
                        execute,
                        preview,
                        confirm_prefix.as_deref(),
                        &older_than,
                        storage,
                    )
                    .await
                }
            }
        }
        Command::Settings(Settings::Get { section }) => {
            let section = section.trim();
            super::settings::get_section_value(app, section).await
        }
        Command::Settings(Settings::Set {
            section,
            revision,
            document,
            ..
        }) => {
            super::settings::set_section_value(
                app,
                principal,
                &section,
                revision,
                document.context("missing runtime settings document")?,
            )
            .await
        }
        Command::Settings(Settings::Reset { section }) => {
            super::settings::reset_section_value(app, principal, &section).await
        }
        Command::Settings(Settings::Control(command)) => match command {
            ControlSettings::Show => Ok(serde_json::to_value(super::settings::read(app).await?)?),
            ControlSettings::Set {
                origin,
                secure_cookie,
                session_lifetime,
                gravatar_base_url,
                revision,
            } => Ok(serde_json::to_value(
                super::settings::update(
                    app,
                    principal,
                    super::settings::ControlSettingsPatch {
                        origin,
                        secure_cookie,
                        session_lifetime,
                        gravatar_base_url,
                        revision,
                    },
                )
                .await?,
            )?),
        },
        Command::Keys(command) => match command {
            Keys::List => Ok(serde_json::to_value(
                super::managed_keys::list_rows(&app.db).await?,
            )?),
            Keys::Create {
                name,
                purpose,
                algorithm,
            } => Ok(serde_json::to_value(
                super::managed_keys::create_record(
                    app,
                    principal,
                    super::managed_keys::ManagedKeyInput {
                        name,
                        purpose,
                        algorithm,
                    },
                )
                .await?,
            )?),
            Keys::Revoke { id } => {
                super::managed_keys::revoke_record(app, principal, id).await?;
                Ok(json!({"revoked": id}))
            }
        },
        Command::Cache(Cache::Status) => app.upload_cache_status().await,
        Command::Cache(Cache::Flush) => app.cache_flush_start().await,
        Command::Pack(Packs::Status) => app.pack_status().await,
        Command::Pack(Packs::Run { mode }) => app.pack_start(&mode).await,
        Command::Pack(Packs::Unpack { id, all, execute }) => {
            app.unpack_start(id, all, execute).await
        }
        Command::Integrity(Integrity::Check { mode, bucket, key }) => {
            app.integrity_start(crate::integrity::Request { mode, bucket, key })
                .await
        }
        Command::Integrity(Integrity::Issues { id, after, limit }) => {
            app.integrity_issues(id, after, limit).await
        }
        Command::Cleanup(Cleanup::Status) => Ok(app.cleanup_status()),
        Command::Cleanup(Cleanup::Run) => app.cleanup_history().await,
        Command::Gc(Gc::Status) => app.gc_status().await,
        Command::Gc(Gc::MarkUnreferenced) => Ok(json!({"marked": app.mark_unreferenced().await?})),
        Command::Gc(Gc::Pause) => {
            app.maintenance_change("gc", true).await?;
            app.gc_status().await
        }
        Command::Gc(Gc::Resume) => {
            app.maintenance_change("gc", false).await?;
            app.gc_status().await
        }
        Command::Gc(Gc::Run) => {
            let cleaned = app.cleanup().await?;
            let reclaimed = app.reclaim().await?;
            Ok(
                json!({"local_cleaned":cleaned,"chunks_reclaimed":reclaimed,"batch_size":app.config.maintenance.batch_size}),
            )
        }
        Command::Maintenance(Maintenance::Status) => app.maintenance_status().await,
        Command::Maintenance(Maintenance::Pause { task }) => {
            app.maintenance_change(&task, true).await
        }
        Command::Maintenance(Maintenance::Resume { task }) => {
            app.maintenance_change(&task, false).await
        }
        Command::Maintenance(Maintenance::Run { task }) => app.maintenance_start(&task).await,
        Command::Maintenance(Maintenance::PackCreation { action }) => {
            anyhow::ensure!(
                ["stop", "resume"].contains(&action.as_str()),
                "invalid pack creation action"
            );
            app.pack_creation_change(action == "stop").await
        }
        Command::Maintenance(mode @ (Maintenance::Enable | Maintenance::Disable)) => {
            let enabled = matches!(mode, Maintenance::Enable);
            let _coord = app.coord.lock().await;
            if !enabled {
                let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='sweep' AND detail->>'dry_run'='false' AND state IN ('queued','running'))").fetch_one(&app.db).await?;
                ensure!(
                    !pending,
                    "pause or finish destructive sweep before disabling maintenance"
                );
            }
            if !enabled {
                app.storage.check_identity(&app.db, false).await?;
            }
            let mut tx = app.db.begin().await?;
            sqlx::query("UPDATE mokyu_meta SET maintenance=$1")
                .bind(enabled)
                .execute(&mut *tx)
                .await?;
            if enabled {
                sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE task_type IN ('purge','pack','unpack') AND state IN ('queued','running')")
                      .execute(&mut *tx).await?;
            }
            super::audit::checkpoint(
                &mut tx,
                "maintenance.mode",
                "deployment",
                json!({"enabled":enabled}),
            )
            .await?;
            tx.commit().await?;
            app.maintenance
                .store(enabled, std::sync::atomic::Ordering::Release);
            Ok(json!({"maintenance":enabled,"active_operations":app.active.lock().unwrap().len()}))
        }
        Command::Task(Tasks::List) => {
            let tasks: Vec<Value> = sqlx::query_scalar(
                "SELECT to_jsonb(t) FROM tasks t ORDER BY created_at DESC LIMIT 100",
            )
            .fetch_all(&app.db)
            .await?;
            Ok(json!(tasks))
        }
        Command::Task(Tasks::Show { id }) => {
            sqlx::query_scalar("SELECT to_jsonb(t) FROM tasks t WHERE id=$1")
                .bind(id)
                .fetch_optional(&app.db)
                .await?
                .context("task not found")
        }
        Command::Task(Tasks::Pause { id }) => app.task_change(id, false).await,
        Command::Task(Tasks::Resume { id }) => app.task_change(id, true).await,
        Command::Bucket(Buckets::Purge {
            name,
            execute: false,
            ..
        }) => app.purge_preview(&name).await,
        Command::Bucket(Buckets::Purge {
            name,
            execute: true,
            confirm_bucket,
            confirmed,
        }) => {
            ensure!(
                confirmed,
                "purge requires preview and confirmation through CLI"
            );
            app.purge_start(
                principal,
                &name,
                confirm_bucket.context("missing bucket confirmation")?,
                None,
            )
            .await
        }
        Command::Bucket(Buckets::List) => {
            let buckets: Vec<Bucket> = sqlx::query_as("SELECT * FROM buckets ORDER BY name")
                .fetch_all(&app.db)
                .await?;
            Ok(serde_json::to_value(buckets)?)
        }
        Command::Bucket(Buckets::Create {
            name,
            project,
            storage,
        }) => {
            let row = super::buckets::create_bucket(
                app,
                principal,
                super::buckets::CreateBucket {
                    name,
                    project_id: project,
                    storage_id: storage,
                },
            )
            .await?;
            Ok(serde_json::to_value(row)?)
        }
        Command::Bucket(Buckets::Delete { name }) => {
            let bucket = app.bucket(&name, true).await?;
            super::buckets::delete_bucket(app, principal, bucket.id, &name, None).await?;
            Ok(json!({"deleted":bucket.id}))
        }
        Command::Bucket(Buckets::Cors { name, document, .. }) => {
            let b = app.bucket(&name, true).await?;
            let rules = document.context("missing CORS document")?;
            app.set_cors(principal, b.id, rules).await
        }
        Command::Credential(command) => {
            use super::keys::{self, CredentialInput, RotateCredential};
            match command {
                Credentials::List { after } => Ok(serde_json::to_value(
                    keys::list_credentials(&app.db, None, &after.unwrap_or_default(), 100).await?,
                )?),
                Credentials::Show { access_key } => Ok(serde_json::to_value(
                    keys::get_credential(&mut *app.db.acquire().await?, &access_key).await?,
                )?),
                Credentials::Create {
                    bucket,
                    read_only,
                    label,
                    expires_in,
                } => {
                    let b = app.bucket(&bucket, true).await?;
                    let row = keys::create_credential(
                        app,
                        principal,
                        CredentialInput {
                            project_id: b.project_id,
                            label: label.unwrap_or_else(|| bucket.clone()),
                            expires_in,
                            grants: vec![super::users::BucketGrant {
                                bucket_id: b.id,
                                actions: Action::role(if read_only { "reader" } else { "writer" }),
                            }],
                        },
                    )
                    .await?;
                    Ok(
                        json!({"access_key":row.access_key,"secret_key":row.secret_key,"bucket":bucket,"writable":!read_only}),
                    )
                }
                Credentials::Grant {
                    access_key,
                    bucket,
                    read_only,
                } => {
                    let b = app.bucket(&bucket, true).await?;
                    keys::change_grant(
                        app,
                        principal,
                        &access_key,
                        b.id,
                        Action::role(if read_only { "reader" } else { "writer" }),
                    )
                    .await?;
                    Ok(json!({"granted":true}))
                }
                Credentials::Revoke { access_key, bucket } => {
                    let b = app.bucket(&bucket, true).await?;
                    keys::change_grant(app, principal, &access_key, b.id, vec![]).await?;
                    Ok(json!({"revoked":true}))
                }
                Credentials::Disable { access_key } => {
                    keys::enable_credential(app, principal, &access_key, false).await?;
                    Ok(json!({"disabled":true}))
                }
                Credentials::Enable { access_key } => {
                    keys::enable_credential(app, principal, &access_key, true).await?;
                    Ok(json!({"enabled":true}))
                }
                Credentials::Delete { access_key } => {
                    keys::revoke_credential(app, principal, &access_key).await?;
                    Ok(json!({"deleted":true}))
                }
                Credentials::Rotate {
                    access_key,
                    overlap,
                    expires_in,
                } => Ok(serde_json::to_value(
                    keys::rotate_credential(
                        app,
                        principal,
                        &access_key,
                        RotateCredential {
                            overlap,
                            expires_in,
                        },
                    )
                    .await?,
                )?),
                Credentials::Update {
                    access_key,
                    document,
                    ..
                } => Ok(serde_json::to_value(
                    keys::update_credential(
                        app,
                        principal,
                        &access_key,
                        serde_json::from_value(document.context("missing key settings")?)?,
                    )
                    .await?,
                )?),
                Credentials::Permissions {
                    access_key,
                    document,
                    ..
                } => Ok(serde_json::to_value(
                    keys::set_grants(
                        app,
                        principal,
                        &access_key,
                        serde_json::from_value(document.context("missing grants")?)?,
                    )
                    .await?,
                )?),
            }
        }
        Command::Token(command) => {
            use super::tokens;
            use crate::cli::Tokens;
            match command {
                Tokens::List { username, after } => {
                    let user = sqlx::query_scalar("SELECT id FROM web_users WHERE username=$1")
                        .bind(username)
                        .fetch_optional(&app.db)
                        .await?
                        .context("user not found")?;
                    Ok(serde_json::to_value(
                        tokens::list_tokens(&app.db, user, after, 100).await?,
                    )?)
                }
                Tokens::Create {
                    username, document, ..
                } => {
                    let user = sqlx::query_scalar("SELECT id FROM web_users WHERE username=$1")
                        .bind(username)
                        .fetch_optional(&app.db)
                        .await?
                        .context("user not found")?;
                    Ok(serde_json::to_value(
                        tokens::create_token(
                            app,
                            principal,
                            user,
                            serde_json::from_value(document.context("missing token settings")?)?,
                        )
                        .await?,
                    )?)
                }
                Tokens::Update { id, document, .. } => {
                    let user = sqlx::query_scalar("SELECT user_id FROM api_tokens WHERE id=$1")
                        .bind(id)
                        .fetch_optional(&app.db)
                        .await?
                        .context("token not found")?;
                    Ok(serde_json::to_value(
                        tokens::update_token(
                            app,
                            principal,
                            user,
                            id,
                            serde_json::from_value(document.context("missing token settings")?)?,
                        )
                        .await?,
                    )?)
                }
                Tokens::Revoke { id } => {
                    tokens::revoke_token(app, principal, id).await?;
                    Ok(json!({"revoked":true}))
                }
            }
        }
        Command::Domain(Domains::List) => {
            let rows:Vec<(String,String)>=sqlx::query_as("SELECT d.host,b.name FROM bucket_domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&app.db).await?;
            Ok(json!(
                rows.into_iter()
                    .map(|(host, bucket)| json!({"host":host,"bucket":bucket}))
                    .collect::<Vec<_>>()
            ))
        }
        Command::Domain(Domains::Set { host, bucket }) => {
            let row = app.bucket(&bucket, true).await?;
            super::buckets::domain_change(app, principal, &host, Some(row.id)).await?;
            Ok(json!({"host":host.to_ascii_lowercase(),"bucket":bucket}))
        }
        Command::Domain(Domains::Delete { host }) => {
            super::buckets::domain_change(app, principal, &host, None).await?;
            Ok(json!({"deleted":true}))
        }
        Command::User(user) => {
            use super::users::{self, CreateUser, ResetPassword, UserPatch};
            let local = principal;
            match user {
                Users::List => {
                    let rows: Vec<users::User> =
                        sqlx::query_as("SELECT * FROM web_users ORDER BY username LIMIT 1000")
                            .fetch_all(&app.db)
                            .await?;
                    Ok(serde_json::to_value(rows)?)
                }
                Users::Create {
                    username,
                    password,
                    role,
                    require_change,
                    ..
                } => Ok(serde_json::to_value(
                    users::create_user(
                        app,
                        &local,
                        CreateUser {
                            username,
                            password: password.context("missing password")?,
                            role,
                            must_change_password: require_change,
                        },
                    )
                    .await?,
                )?),
                Users::Password {
                    username,
                    password,
                    require_change,
                    ..
                } => {
                    let user = users::find(app, &username).await?;
                    users::reset_password(
                        app,
                        &local,
                        user.id,
                        ResetPassword {
                            password: password.context("missing password")?,
                            must_change_password: require_change,
                        },
                    )
                    .await?;
                    Ok(json!({"updated":username}))
                }
                Users::Enable { username } => {
                    let user = users::find(app, &username).await?;
                    Ok(serde_json::to_value(
                        users::update_user(
                            app,
                            &local,
                            user.id,
                            UserPatch {
                                enabled: Some(true),
                                ..Default::default()
                            },
                        )
                        .await?,
                    )?)
                }
                Users::Disable { username } => {
                    let user = users::find(app, &username).await?;
                    users::update_user(
                        app,
                        &local,
                        user.id,
                        UserPatch {
                            enabled: Some(false),
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(json!({"disabled":username}))
                }
                Users::Role { username, role } => {
                    let user = users::find(app, &username).await?;
                    Ok(serde_json::to_value(
                        users::update_user(
                            app,
                            &local,
                            user.id,
                            UserPatch {
                                role: Some(role),
                                ..Default::default()
                            },
                        )
                        .await?,
                    )?)
                }
                Users::Delete { username } => {
                    let user = users::find(app, &username).await?;
                    users::delete_user(app, &local, user.id).await?;
                    Ok(json!({"deleted":username}))
                }
                Users::Membership {
                    username,
                    project,
                    document,
                    ..
                } => {
                    let user = users::find(app, &username).await?;
                    users::save_member(
                        app,
                        &local,
                        project,
                        user.id,
                        Some(serde_json::from_value(
                            document.context("missing membership document")?,
                        )?),
                    )
                    .await?;
                    Ok(json!({"updated":username,"project_id":project}))
                }
                Users::Leave { username, project } => {
                    let user = users::find(app, &username).await?;
                    users::save_member(app, &local, project, user.id, None).await?;
                    Ok(json!({"removed":username,"project_id":project}))
                }
            }
        }
    }
}
