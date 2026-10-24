use crate::model::Id;
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
    if matches!(&command, Command::Cluster(_)) {
        app.cluster()?;
    }
    if !super::audit::active()
        && let Some(action) = command.audit_action()
    {
        let id = super::audit::begin(app.audit.as_ref(), principal, action, None).await?;
        let result = super::audit::scope(id, execute_inner(app, principal, command)).await;
        super::audit::finish(
            app.audit.as_ref(),
            id,
            if result.is_ok() { 200 } else { 400 },
            false,
        )
        .await;
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
            let local_control = matches!(principal, Principal::Bootstrap | Principal::Local)
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
                Projects::List => Ok(serde_json::to_value(
                    projects::list_all(app.administration.as_ref()).await?,
                )?),
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
                app.runtime.health().await?;
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
            Cluster::Create { name, role } => Ok(serde_json::to_value(
                app.cluster()?
                    .provision_node(principal, &name, &role, &app.secrets.credential_key)
                    .await?,
            )?),
            Cluster::Rotate { name, confirm } => {
                ensure!(confirm, "node credential rotation requires --confirm");
                Ok(serde_json::to_value(
                    app.cluster()?
                        .rotate_node(principal, &name, &app.secrets.credential_key)
                        .await?,
                )?)
            }
            Cluster::Status => Ok(serde_json::to_value(
                crate::control::cluster::overview(app).await?,
            )?),
            Cluster::Mode { mode } => {
                app.change_mode(principal, &mode).await?;
                Ok(json!({"mode":mode}))
            }
            Cluster::Nodes {
                name,
                limit,
                after,
                role,
                contact,
                work,
                configuration,
                section,
            } => {
                let filter = crate::model::node::NodeFilter {
                    name,
                    limit,
                    after,
                    role,
                    contact,
                    work,
                    configuration,
                    section,
                };
                filter.cursor()?;
                let desired = crate::model::node::desired(app).await?;
                Ok(serde_json::to_value(
                    app.cluster()?.nodes(&filter, &desired).await?,
                )?)
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
                app.cluster()?.release(&name).await?;
                Ok(json!({"released": name}))
            }
        },
        Command::Setup(Setup::Token) => {
            Ok(json!({"token": super::account::setup_token(app).await?}))
        }
        Command::Storages(command) => {
            let public = |row: super::storage::StorageView| -> Result<Value> {
                let mut value = serde_json::to_value(row)?;
                value.as_object_mut().unwrap().remove("created_at");
                Ok(value)
            };
            match command {
                Storages::List => {
                    let rows = app.storage_records.list().await?;
                    Ok(json!(
                        rows.into_iter().map(public).collect::<Result<Vec<_>>>()?
                    ))
                }
                Storages::Check { name } => {
                    let row = app
                        .storage_records
                        .named(&name)
                        .await?
                        .context("storage not found")?;
                    app.storage.refresh_store(row.id).await?;
                    ensure!(row.enabled, "storage is disabled");
                    app.storage.check_storage(row.id).await?;
                    public(row)
                }
                Storages::Show { name } => public(
                    app.storage_records
                        .named(&name)
                        .await?
                        .context("storage not found")?,
                ),
                Storages::Key { command } => match command {
                    StorageKey::Set { storage, key } => Ok(serde_json::to_value(
                        super::storage::set_current_key(app, storage, Some(key)).await?,
                    )?),
                    StorageKey::Clear { storage } => Ok(serde_json::to_value(
                        super::storage::set_current_key(app, storage, None).await?,
                    )?),
                },
                Storages::Profile { command } => match command {
                    StorageProfiles::Presets { name } => Ok(serde_json::to_value(
                        super::storage_policy::named(app, &name).await?,
                    )?),
                    StorageProfiles::Preset {
                        name,
                        preset,
                        draft,
                    } => {
                        let preview = super::storage_policy::preset(app, &name, &preset).await?;
                        Ok(if draft {
                            serde_json::to_value(preview.profile)?
                        } else {
                            serde_json::to_value(preview)?
                        })
                    }
                    StorageProfiles::Preview { name, document, .. } => {
                        let profile = serde_json::from_value(
                            document.context("missing storage profile document")?,
                        )?;
                        Ok(serde_json::to_value(
                            super::storage_policy::preview(app, &name, profile).await?,
                        )?)
                    }
                    StorageProfiles::Show { name } => {
                        let storage_id: uuid::Uuid = app
                            .storage_records
                            .id(&name, None)
                            .await?
                            .context("storage not found")?;
                        Ok(serde_json::to_value(
                            app.storage_profiles.load(storage_id).await?,
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
                    let storage_id: uuid::Uuid = app
                        .storage_records
                        .id(&storage, storage.parse().ok())
                        .await?
                        .context("storage not found")?;
                    match command {
                        RewriteCommand::Preview { target_key } => {
                            app.rewrite_start(storage_id, target_key.as_deref(), false, None)
                                .await
                        }
                        RewriteCommand::Start {
                            target_key,
                            confirm,
                        } => {
                            app.rewrite_start(
                                storage_id,
                                target_key.as_deref(),
                                true,
                                Some(&confirm),
                            )
                            .await
                        }
                        RewriteCommand::Status => Ok(app
                            .task_control
                            .rewrite_status(storage_id)
                            .await?
                            .context("rewrite task not found")?),
                        RewriteCommand::Pause | RewriteCommand::Resume | RewriteCommand::Retry => {
                            let task: uuid::Uuid = app
                                .task_control
                                .rewrite_task(storage_id)
                                .await?
                                .context("rewrite task not found")?;
                            let resume =
                                matches!(command, RewriteCommand::Resume | RewriteCommand::Retry);
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
                        app.managed_keys
                            .resolve_block(&key)
                            .await?
                            .map(|key| key.id)
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
                            app.managed_keys
                                .resolve_block(&key)
                                .await?
                                .map(|key| key.id)
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
            Keys::ReplacePreview {
                source,
                target,
                name,
            } => Ok(serde_json::to_value(
                super::managed_keys::preview_replacement(
                    app,
                    principal,
                    source,
                    crate::model::key_replacement::KeyReplacementInput { target, name },
                )
                .await?,
            )?),
            Keys::Replacement { id } => Ok(serde_json::to_value(
                super::managed_keys::replacement_status(app, principal, id).await?,
            )?),
            Keys::Replace { id } => Ok(serde_json::to_value(
                super::managed_keys::replace_associated(app, principal, id).await?,
            )?),
            Keys::List => Ok(serde_json::to_value(
                super::managed_keys::list_rows(app.managed_keys.as_ref(), principal).await?,
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
        Command::Cache(Cache::Flush { .. }) => app.cache_flush_start().await,
        Command::Pack(Packs::Status) => app.pack_status().await,
        Command::Pack(Packs::Run { mode }) => app.pack_start(&mode).await,
        Command::Pack(Packs::Unpack { id, all, execute }) => {
            app.unpack_start(id, all, execute).await
        }
        Command::Integrity(Integrity::Check { mode, bucket, key }) => {
            app.integrity_start(crate::worker::integrity::Request { mode, bucket, key })
                .await
        }
        Command::Integrity(Integrity::Issues { id, after, limit }) => {
            app.integrity_issues(id, Id::parse(&after, app.config.database.backend)?, limit)
                .await
        }
        Command::Integrity(Integrity::Quarantines {
            storage,
            source,
            after,
            limit,
        }) => Ok(serde_json::to_value(
            app.source_list(
                principal,
                crate::model::source::SourceQuery {
                    storage_id: storage,
                    source,
                    after,
                    limit: Some(limit),
                },
            )
            .await?,
        )?),
        Command::Integrity(
            command @ (Integrity::Quarantine { .. } | Integrity::Recheck { .. }),
        ) => {
            let recheck = matches!(&command, Integrity::Recheck { .. });
            let (storage_id, source, id, confirm) = match command {
                Integrity::Quarantine {
                    storage,
                    source,
                    id,
                    confirm,
                }
                | Integrity::Recheck {
                    storage,
                    source,
                    id,
                    confirm,
                } => (storage, source, id, confirm),
                _ => unreachable!(),
            };
            let operation = app
                .source_prepare(
                    principal,
                    crate::model::source::SourceScope {
                        storage_id,
                        source,
                        id,
                    },
                    recheck,
                )
                .await?;
            let fingerprint = operation.snapshot.fingerprint()?;
            if let Some(confirm) = confirm {
                ensure!(confirm == fingerprint, s3s::s3_error!(PreconditionFailed));
                app.source_start(operation).await
            } else {
                Ok(
                    json!({"impact":operation.snapshot.summary()?,"confirm":fingerprint,"effect":if recheck { "full verification before clearing quarantine" } else { "reject new references to this source" }}),
                )
            }
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
            if app.draining.load(std::sync::atomic::Ordering::Acquire) {
                return Err(s3s::s3_error!(ServiceUnavailable, "gateway is draining").into());
            }
            if !enabled {
                app.ensure_mode()?;
                let pending = app.maintenance_store.destructive_sweep().await?;
                ensure!(
                    !pending,
                    "pause or finish destructive sweep before disabling maintenance"
                );
            }
            if !enabled && app.storage.enabled() {
                app.storage.check_identity().await?;
            }
            app.maintenance_store.mode(enabled).await?;
            app.maintenance
                .store(enabled, std::sync::atomic::Ordering::Release);
            Ok(json!({"maintenance":enabled,"active_operations":app.active_operations()}))
        }
        Command::Task(Tasks::List) => {
            let mut tasks = app.task_control.all().await?;
            for task in &mut tasks {
                if let Some(detail) = task["detail"].as_object_mut() {
                    detail.remove("source_operation");
                    detail.remove("source_inputs");
                }
            }
            Ok(json!(tasks))
        }
        Command::Task(Tasks::Show { id }) => {
            let mut task = app.task_control.raw(id).await?.context("task not found")?;
            if let Some(detail) = task["detail"].as_object_mut() {
                detail.remove("source_operation");
                detail.remove("source_inputs");
            }
            Ok(task)
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
            let buckets: Vec<Bucket> = app.buckets.all().await?;
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
                    keys::list_credentials(
                        app.credentials.as_ref(),
                        None,
                        &after.unwrap_or_default(),
                        100,
                    )
                    .await?,
                )?),
                Credentials::Show { access_key } => Ok(serde_json::to_value(
                    keys::get_credential(app.credentials.as_ref(), &access_key).await?,
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
                    let user = app
                        .administration
                        .find_user(&username)
                        .await?
                        .context("user not found")?
                        .id;
                    Ok(serde_json::to_value(
                        tokens::list_tokens(app.credentials.as_ref(), user, after, 100).await?,
                    )?)
                }
                Tokens::Create {
                    username, document, ..
                } => {
                    let user = app
                        .administration
                        .find_user(&username)
                        .await?
                        .context("user not found")?
                        .id;
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
                    let user = app
                        .credentials
                        .token_user(id)
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
            let rows = app.buckets.domain_names().await?;
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
                    let rows = app.administration.all_users().await?;
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
