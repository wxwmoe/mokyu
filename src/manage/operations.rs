use crate::{
    admin::{
        Buckets, Cache, Cleanup, Cluster, Command, Credentials, Domains, Gc, Integrity, Keys,
        Maintenance, Packs, Projects, Stores, Tasks, Users,
    },
    app::{App, Bucket},
    authorization::{Action, Principal},
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
pub async fn execute(app: &Arc<App>, command: Command) -> Result<Value> {
    if !super::audit::active()
        && let Some(action) = command.audit_action()
    {
        let id = super::audit::begin(&app.db, &Principal::Local, action, None).await?;
        let result = super::audit::scope(id, execute_inner(app, command)).await;
        super::audit::finish(&app.db, id, if result.is_ok() { 200 } else { 400 }, false).await;
        return result;
    }
    execute_inner(app, command).await
}
async fn execute_inner(app: &Arc<App>, command: Command) -> Result<Value> {
    match command {
        Command::Quota(command) => {
            use crate::admin::Quotas;
            let result = match command {
                Quotas::Show { kind, id } => {
                    super::quotas::read(app, &Principal::Local, &kind, id).await?
                }
                Quotas::Set {
                    kind, id, document, ..
                } => {
                    super::quotas::save(
                        app,
                        &Principal::Local,
                        &kind,
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
        Command::Cluster(command) => match command {
            Cluster::Status => {
                let mode: String =
                    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
                        .fetch_one(&app.db)
                        .await?;
                let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE enabled")
                    .fetch_one(&app.db)
                    .await?;
                let local_targets: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM storage_targets WHERE type='local'")
                        .fetch_one(&app.db)
                        .await?;
                Ok(json!({"mode": mode, "enabled_nodes": nodes, "local_targets": local_targets}))
            }
            Cluster::Mode { mode } => {
                let current: String =
                    sqlx::query_scalar("SELECT deployment_mode FROM mokyu_meta WHERE singleton")
                        .fetch_one(&app.db)
                        .await?;
                if current == mode {
                    return Ok(json!({"mode": mode}));
                }
                if mode == "cluster" {
                    let local: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM storage_targets WHERE type='local')",
                    )
                    .fetch_one(&app.db)
                    .await?;
                    ensure!(
                        !local,
                        "local storage targets exist; migrate them to S3 and update their store before enabling cluster mode"
                    );
                } else {
                    let other_nodes: bool =
                        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled)")
                            .fetch_one(&app.db)
                            .await?;
                    ensure!(
                        !other_nodes,
                        "disable all registered cluster nodes before returning to single mode"
                    );
                }
                sqlx::query("UPDATE mokyu_meta SET deployment_mode=$1 WHERE singleton")
                    .bind(&mode)
                    .execute(&app.db)
                    .await?;
                Ok(json!({"mode": mode}))
            }
            Cluster::Nodes => {
                let rows: Vec<(
                    uuid::Uuid,
                    String,
                    String,
                    Option<String>,
                    bool,
                    Option<chrono::DateTime<chrono::Utc>>,
                    chrono::DateTime<chrono::Utc>,
                )> = sqlx::query_as(
                    "SELECT id,name,kind,advertise_url,enabled,last_seen_at,created_at FROM nodes ORDER BY name",
                )
                .fetch_all(&app.db)
                .await?;
                Ok(json!(rows
                    .into_iter()
                    .map(|(id, name, kind, advertise_url, enabled, last_seen_at, created_at)| {
                        json!({"id":id,"name":name,"kind":kind,"advertise_url":advertise_url,"enabled":enabled,"last_seen_at":last_seen_at,"created_at":created_at})
                    })
                    .collect::<Vec<_>>()))
            }
            Cluster::Token {
                name,
                kind,
                advertise_url,
                expires_in,
            } => {
                ensure!(
                    (1..=128).contains(&name.trim().len())
                        && !name.chars().any(char::is_control)
                        && !name.contains('/'),
                    "node name must contain 1..128 safe bytes"
                );
                ensure!(
                    kind != "control" || advertise_url.is_some(),
                    "control nodes require --advertise-url"
                );
                if let Some(url) = &advertise_url {
                    let uri: hyper::Uri = url.parse().context("invalid advertise URL")?;
                    ensure!(
                        uri.scheme_str().is_some() && uri.authority().is_some(),
                        "advertise URL must be absolute"
                    );
                }
                let seconds = crate::config::seconds(&expires_in)?;
                ensure!(
                    (60..=30 * 86400).contains(&seconds),
                    "token lifetime must be 1m..30d"
                );
                let token = crate::cluster::join_token()?;
                let hash = crate::cluster::join_token_hash(&token)?;
                let id = uuid::Uuid::new_v4();
                let expires_at = chrono::Utc::now()
                    + chrono::Duration::seconds(
                        i64::try_from(seconds).context("token lifetime overflow")?,
                    );
                let mut tx = app.db.begin().await?;
                sqlx::query("INSERT INTO nodes(id,name,kind,advertise_url) VALUES($1,$2,$3,$4)")
                    .bind(id)
                    .bind(name.trim())
                    .bind(&kind)
                    .bind(advertise_url)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("INSERT INTO node_join_tokens(id,node_id,token_hash,expires_at) VALUES($1,$2,$3,$4)")
                    .bind(uuid::Uuid::new_v4())
                    .bind(id)
                    .bind(hash.as_slice())
                    .bind(expires_at)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                Ok(
                    json!({"node_id":id,"name":name.trim(),"kind":kind,"token":token,"expires_at":expires_at}),
                )
            }
            Cluster::Revoke { id } => {
                let changed = sqlx::query(
                    "UPDATE nodes SET enabled=false,updated_at=now() WHERE id=$1 AND enabled",
                )
                .bind(id)
                .execute(&app.db)
                .await?
                .rows_affected();
                ensure!(changed == 1, "node not found or already disabled");
                sqlx::query("UPDATE node_join_tokens SET consumed_at=coalesce(consumed_at,now()) WHERE node_id=$1 AND consumed_at IS NULL")
                    .bind(id)
                    .execute(&app.db)
                    .await?;
                Ok(json!({"revoked": id}))
            }
        },
        Command::Stores(command) => {
            type Row = (
                uuid::Uuid,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                String,
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
                    "enabled": row.8,
                    "updated_at": row.9,
                })
            };
            match command {
                Stores::List => {
                    let rows: Vec<Row> = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,enabled,updated_at FROM storage_targets ORDER BY name")
                        .fetch_all(&app.db)
                        .await?;
                    Ok(json!(rows.into_iter().map(public).collect::<Vec<_>>()))
                }
                Stores::Check { name } => {
                    let row: Row = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,enabled,updated_at FROM storage_targets WHERE name=$1")
                        .bind(&name)
                        .fetch_optional(&app.db)
                        .await?
                        .context("storage target not found")?;
                    Ok(public(row))
                }
                Stores::Create {
                    name,
                    r#type,
                    path,
                    endpoint,
                    bucket,
                    region,
                    prefix,
                    access_key,
                    secret_key,
                } => {
                    ensure!(
                        (1..=128).contains(&name.trim().len()),
                        "storage name must contain 1..128 bytes"
                    );
                    let id = uuid::Uuid::new_v4();
                    let (path, endpoint, bucket, region, access, secret) = if r#type == "local" {
                        ensure!(
                            endpoint.is_none()
                                && bucket.is_none()
                                && region.is_none()
                                && access_key.is_none()
                                && secret_key.is_none(),
                            "local stores cannot include S3 settings"
                        );
                        (
                            Some(path.unwrap_or_else(|| "/data/storage".into())),
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    } else {
                        let endpoint = endpoint.context("S3 endpoint is required")?;
                        let bucket = bucket.context("S3 bucket is required")?;
                        let region = region.context("S3 region is required")?;
                        let access_key = access_key.context("S3 access key is required")?;
                        let secret_key = secret_key.context("S3 secret key is required")?;
                        ensure!(
                            !endpoint.is_empty() && !bucket.is_empty() && !region.is_empty(),
                            "S3 connection settings must not be empty"
                        );
                        let encrypted_access = crate::codec::protect(
                            access_key.as_bytes(),
                            &app.secrets.credential_key,
                            format!("storage-target-{id}-access").as_bytes(),
                        )?;
                        let encrypted_secret = crate::codec::protect(
                            secret_key.as_bytes(),
                            &app.secrets.credential_key,
                            format!("storage-target-{id}-secret").as_bytes(),
                        )?;
                        (
                            None,
                            Some(endpoint),
                            Some(bucket),
                            Some(region),
                            Some(encrypted_access),
                            Some(encrypted_secret),
                        )
                    };
                    let path = path.map(|value| value.to_string_lossy().into_owned());
                    sqlx::query("INSERT INTO storage_targets(id,name,type,path,endpoint_url,remote_bucket,region,prefix,access_key_encrypted,secret_key_encrypted) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
                        .bind(id)
                        .bind(name.trim())
                        .bind(&r#type)
                        .bind(path)
                        .bind(endpoint)
                        .bind(bucket)
                        .bind(region)
                        .bind(prefix)
                        .bind(access)
                        .bind(secret)
                        .execute(&app.db)
                        .await?;
                    let row: Row = sqlx::query_as("SELECT id,name,type,path,endpoint_url,remote_bucket,region,prefix,enabled,updated_at FROM storage_targets WHERE id=$1")
                        .bind(id)
                        .fetch_one(&app.db)
                        .await?;
                    Ok(public(row))
                }
                Stores::Sweep {
                    execute,
                    preview,
                    confirm_prefix,
                    older_than,
                } => {
                    app.sweep_start(execute, preview, confirm_prefix.as_deref(), &older_than)
                        .await
                }
            }
        }
        Command::Keys(command) => match command {
            Keys::List => Ok(serde_json::to_value(
                super::managed_keys::list_rows(&app.db).await?,
            )?),
            Keys::Create { name, purpose } => Ok(serde_json::to_value(
                super::managed_keys::create_record(
                    app,
                    &crate::authorization::Principal::Local,
                    super::managed_keys::ManagedKeyInput { name, purpose },
                )
                .await?,
            )?),
            Keys::Revoke { id } => {
                super::managed_keys::revoke_record(
                    app,
                    &crate::authorization::Principal::Local,
                    id,
                )
                .await?;
                Ok(json!({"revoked": id}))
            }
        },
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
                json!({"local_cleaned":cleaned,"chunks_reclaimed":reclaimed,"batch_size":app.config.gc.batch_size}),
            )
        }
        Command::Maintenance(Maintenance::Status) => app.maintenance_status().await,
        Command::Maintenance(Maintenance::Pause { kind }) => {
            app.maintenance_change(&kind, true).await
        }
        Command::Maintenance(Maintenance::Resume { kind }) => {
            app.maintenance_change(&kind, false).await
        }
        Command::Maintenance(Maintenance::Run { kind }) => app.maintenance_start(&kind).await,
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
                let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='sweep' AND detail->>'dry_run'='false' AND state IN ('queued','running'))").fetch_one(&app.db).await?;
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
                sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE kind IN ('purge','pack','unpack') AND state IN ('queued','running')")
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
                &Principal::Local,
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
        Command::Bucket(Buckets::Create { name, project }) => {
            let row = super::buckets::create_bucket(
                app,
                &Principal::Local,
                super::buckets::CreateBucket {
                    name,
                    project_id: project,
                },
            )
            .await?;
            Ok(serde_json::to_value(row)?)
        }
        Command::Bucket(Buckets::Delete { name }) => {
            let bucket = app.bucket(&name, true).await?;
            super::buckets::delete_bucket(app, &Principal::Local, bucket.id, &name, None).await?;
            Ok(json!({"deleted":bucket.id}))
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
            let row = app.bucket(&bucket, true).await?;
            super::buckets::domain_change(app, &Principal::Local, &host, Some(row.id)).await?;
            Ok(json!({"host":host.to_ascii_lowercase(),"bucket":bucket}))
        }
        Command::Domain(Domains::Delete { host }) => {
            super::buckets::domain_change(app, &Principal::Local, &host, None).await?;
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
