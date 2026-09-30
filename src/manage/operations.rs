use crate::{
    admin::{
        Backend, Buckets, Cache, Cleanup, Command, Credentials, Domains, Gc, Integrity,
        Maintenance, Packs, Projects, Tasks, Users,
    },
    app::{App, Bucket},
    authorization::{Action, Principal},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;
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
pub async fn execute(app: &Arc<App>, command: Command) -> Result<Value> {
    match command {
        Command::Project(project) => {
            use super::projects::{self, ProjectInput};
            match project {
                Projects::List => Ok(serde_json::to_value(projects::list_all(&app.db).await?)?),
                Projects::Create { name } => Ok(serde_json::to_value(
                    projects::save(
                        app,
                        &Principal::Local,
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
                        &Principal::Local,
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
                    projects::remove(app, &Principal::Local, id).await?;
                    Ok(json!({"deleted": id}))
                }
                Projects::Mode { enabled } => {
                    projects::mode(app, &Principal::Local, enabled).await?;
                    Ok(json!({"enabled": enabled}))
                }
            }
        }
        Command::Status => app.status().await,
        Command::Cache(Cache::Status) => app.upload_cache_status().await,
        Command::Cache(Cache::Flush) => app.cache_flush_start().await,
        Command::Pack(Packs::Status) => app.pack_status().await,
        Command::Pack(Packs::Run { kind }) => app.pack_start(&kind).await,
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
        Command::Gc(Gc::Pause) => {
            sqlx::query("UPDATE mokyu_meta SET gc_paused=true")
                .execute(&app.db)
                .await?;
            app.gc_status().await
        }
        Command::Gc(Gc::Resume) => {
            sqlx::query("UPDATE mokyu_meta SET gc_paused=false")
                .execute(&app.db)
                .await?;
            app.gc_status().await
        }
        Command::Gc(Gc::Run) => {
            let cleaned = app.cleanup().await?;
            let reclaimed = app.reclaim().await?;
            Ok(
                json!({"local_cleaned":cleaned,"chunks_reclaimed":reclaimed,"batch_size":app.config.gc.batch_size}),
            )
        }
        Command::Maintenance(mode) => {
            let enabled = matches!(mode, Maintenance::Enable);
            if !enabled {
                let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='sweep' AND detail->>'dry_run'='false' AND state IN ('queued','running'))").fetch_one(&app.db).await?;
                ensure!(
                    !pending,
                    "pause or finish destructive sweep before disabling maintenance"
                );
            }
            let _coord = app.coord.lock().await;
            if !enabled {
                app.storage.check_identity(&app.db, false).await?;
            }
            let mut tx = app.db.begin().await?;
            sqlx::query("UPDATE mokyu_meta SET maintenance=$1")
                .bind(enabled)
                .execute(&mut *tx)
                .await?;
            if enabled {
                sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE kind IN ('purge','pack','unpack') AND state IN ('queued','running')")
                    .execute(&mut *tx).await?;
            }
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
        Command::Backend(Backend::Sweep {
            execute,
            preview,
            confirm_prefix,
            older_than,
        }) => {
            app.sweep_start(execute, preview, confirm_prefix.as_deref(), &older_than)
                .await
        }
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
                &name,
                confirm_bucket.context("missing bucket confirmation")?,
            )
            .await
        }
        Command::Bucket(Buckets::List) => {
            let buckets: Vec<Bucket> = sqlx::query_as("SELECT * FROM buckets ORDER BY name")
                .fetch_all(&app.db)
                .await?;
            Ok(serde_json::to_value(buckets)?)
        }
        Command::Bucket(Buckets::Create { name }) => {
            app.writable()?;
            ensure!(
                s3s::path::check_bucket_name(&name),
                "invalid S3 bucket name"
            );
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO buckets(id,name) VALUES($1,$2)")
                .bind(id)
                .bind(&name)
                .execute(&app.db)
                .await?;
            Ok(json!({"id":id,"name":name}))
        }
        Command::Bucket(Buckets::Delete { name }) => {
            let b = app.bucket(&name, true).await?;
            let _coord = app.coord.lock().await;
            let mut tx = app.db.begin().await?;
            sqlx::query("SELECT id FROM buckets WHERE id=$1 FOR UPDATE")
                .bind(b.id)
                .execute(&mut *tx)
                .await?;
            let nonempty:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(b.id).fetch_one(&mut *tx).await?;
            ensure!(
                !nonempty,
                "bucket has objects, writes or multipart records; use purge or wait for cleanup"
            );
            sqlx::query("DELETE FROM objects WHERE bucket_id=$1")
                .bind(b.id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM buckets WHERE id=$1")
                .bind(b.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(json!({"deleted":b.id}))
        }
        Command::Bucket(Buckets::Cors { name, document, .. }) => {
            let b = app.bucket(&name, true).await?;
            let rules = document.context("missing CORS document")?;
            app.set_cors(&Principal::Local, b.id, rules).await
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
                        &Principal::Local,
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
                        &Principal::Local,
                        &access_key,
                        b.id,
                        Action::role(if read_only { "reader" } else { "writer" }),
                    )
                    .await?;
                    Ok(json!({"granted":true}))
                }
                Credentials::Revoke { access_key, bucket } => {
                    let b = app.bucket(&bucket, true).await?;
                    keys::change_grant(app, &Principal::Local, &access_key, b.id, vec![]).await?;
                    Ok(json!({"revoked":true}))
                }
                Credentials::Disable { access_key } => {
                    keys::enable_credential(app, &Principal::Local, &access_key, false).await?;
                    Ok(json!({"disabled":true}))
                }
                Credentials::Enable { access_key } => {
                    keys::enable_credential(app, &Principal::Local, &access_key, true).await?;
                    Ok(json!({"enabled":true}))
                }
                Credentials::Delete { access_key } => {
                    keys::revoke_credential(app, &Principal::Local, &access_key).await?;
                    Ok(json!({"deleted":true}))
                }
                Credentials::Rotate {
                    access_key,
                    overlap,
                    expires_in,
                } => Ok(serde_json::to_value(
                    keys::rotate_credential(
                        app,
                        &Principal::Local,
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
                        &Principal::Local,
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
                        &Principal::Local,
                        &access_key,
                        serde_json::from_value(document.context("missing grants")?)?,
                    )
                    .await?,
                )?),
            }
        }
        Command::Token(command) => {
            use super::tokens;
            use crate::admin::Tokens;
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
                            &Principal::Local,
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
                            &Principal::Local,
                            user,
                            id,
                            serde_json::from_value(document.context("missing token settings")?)?,
                        )
                        .await?,
                    )?)
                }
                Tokens::Revoke { id } => {
                    tokens::revoke_token(app, &Principal::Local, id).await?;
                    Ok(json!({"revoked":true}))
                }
            }
        }
        Command::Domain(Domains::List) => {
            let rows:Vec<(String,String)>=sqlx::query_as("SELECT d.host,b.name FROM domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&app.db).await?;
            Ok(json!(
                rows.into_iter()
                    .map(|(host, bucket)| json!({"host":host,"bucket":bucket}))
                    .collect::<Vec<_>>()
            ))
        }
        Command::Domain(Domains::Set { host, bucket }) => {
            let host = host.to_ascii_lowercase();
            ensure!(
                !host.is_empty()
                    && host.len() <= 253
                    && host
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b".-:".contains(&b)),
                "invalid media host"
            );
            let b = app.bucket(&bucket, true).await?;
            sqlx::query("INSERT INTO domains(host,bucket_id) VALUES($1,$2) ON CONFLICT(host) DO UPDATE SET bucket_id=excluded.bucket_id").bind(&host).bind(b.id).execute(&app.db).await?;
            Ok(json!({"host":host,"bucket":bucket}))
        }
        Command::Domain(Domains::Delete { host }) => {
            sqlx::query("DELETE FROM domains WHERE host=$1")
                .bind(host.to_ascii_lowercase())
                .execute(&app.db)
                .await?;
            Ok(json!({"deleted":true}))
        }
        Command::User(user) => {
            use super::users::{self, CreateUser, ResetPassword, UserPatch};
            let local = Principal::Local;
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
