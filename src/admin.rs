use crate::{
    app::{App, Bucket},
    codec,
};
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::IsTerminal,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
use uuid::Uuid;

#[derive(Subcommand, Serialize, Deserialize)]
pub enum Command {
    Status,
    #[command(subcommand)]
    Bucket(Buckets),
    #[command(subcommand)]
    Credential(Credentials),
    #[command(subcommand)]
    Domain(Domains),
    #[command(subcommand)]
    User(Users),
    #[command(subcommand)]
    Gc(Gc),
    #[command(subcommand)]
    Maintenance(Maintenance),
    #[command(subcommand)]
    Task(Tasks),
    #[command(subcommand)]
    Backend(Backend),
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Buckets {
    List,
    Create {
        name: String,
    },
    Delete {
        name: String,
    },
    Cors {
        name: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Purge {
        name: String,
        #[arg(long)]
        execute: bool,
        #[arg(skip)]
        confirm_bucket: Option<Uuid>,
        #[arg(skip)]
        confirmed: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Gc {
    Status,
    Run,
    Pause,
    Resume,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Maintenance {
    Enable,
    Disable,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Tasks {
    List,
    Show { id: Uuid },
    Pause { id: Uuid },
    Resume { id: Uuid },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Backend {
    Sweep {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        preview: Option<Uuid>,
        #[arg(skip)]
        confirm_prefix: Option<String>,
        #[arg(long, default_value = "48h")]
        older_than: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Credentials {
    List,
    Create {
        bucket: String,
        #[arg(long)]
        read_only: bool,
    },
    Grant {
        access_key: String,
        bucket: String,
        #[arg(long)]
        read_only: bool,
    },
    Revoke {
        access_key: String,
        bucket: String,
    },
    Disable {
        access_key: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Domains {
    List,
    Set { host: String, bucket: String },
    Delete { host: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Users {
    List,
    Create {
        username: String,
        #[arg(long)]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Password {
        username: String,
        #[arg(long)]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Disable {
        username: String,
    },
    Delete {
        username: String,
    },
}

pub fn random_secret() -> Result<String> {
    let mut bytes = [0; 32];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    Ok(hex::encode(bytes))
}
async fn send_frame(socket: &mut UnixStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= 1024 * 1024, "admin message exceeds limit");
    socket.write_u32(bytes.len() as u32).await?;
    socket.write_all(&bytes).await?;
    Ok(())
}
async fn receive_frame(socket: &mut UnixStream) -> Result<Vec<u8>> {
    let len = socket.read_u32().await?;
    ensure!(len <= 1024 * 1024, "admin message exceeds limit");
    let mut bytes = vec![0; len as usize];
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}
pub async fn client(path: &Path, mut command: Command) -> Result<()> {
    match &mut command {
        Command::Bucket(Buckets::Purge {
            name,
            execute: true,
            confirm_bucket,
            confirmed,
        }) => {
            ensure!(
                std::io::stdin().is_terminal(),
                "destructive commands require a terminal; use docker exec -it"
            );
            let preview = rpc(
                path,
                &Command::Bucket(Buckets::Purge {
                    name: name.clone(),
                    execute: false,
                    confirm_bucket: None,
                    confirmed: false,
                }),
            )
            .await?;
            eprintln!("DANGER: {}", serde_json::to_string_pretty(&preview)?);
            let id: Uuid = serde_json::from_value(preview["bucket_id"].clone())?;
            confirm(name)?;
            *confirm_bucket = Some(id);
            *confirmed = true;
        }
        Command::Backend(Backend::Sweep {
            execute: true,
            preview,
            confirm_prefix,
            ..
        }) => {
            ensure!(
                std::io::stdin().is_terminal(),
                "destructive commands require a terminal; use docker exec -it"
            );
            let task = rpc(
                path,
                &Command::Task(Tasks::Show {
                    id: preview.context("--preview requires a completed dry-run task")?,
                }),
            )
            .await?;
            ensure!(
                task["kind"] == "sweep"
                    && task["state"] == "completed"
                    && task["detail"]["dry_run"] == true,
                "a completed dry-run preview is required"
            );
            eprintln!("DANGER: {}", serde_json::to_string_pretty(&task)?);
            let prefix = task["detail"]["prefix"]
                .as_str()
                .context("preview prefix missing")?;
            confirm(prefix)?;
            *confirm_prefix = Some(prefix.to_owned());
        }
        Command::User(
            Users::Create {
                password_stdin,
                password,
                ..
            }
            | Users::Password {
                password_stdin,
                password,
                ..
            },
        ) => {
            ensure!(
                *password_stdin,
                "use --password-stdin; passwords are not accepted on the command line"
            );
            let mut text = String::new();
            std::io::stdin().read_line(&mut text)?;
            *password = Some(text.trim_end_matches(['\r', '\n']).to_owned());
        }
        Command::Bucket(Buckets::Cors { file, document, .. }) => {
            let meta = std::fs::metadata(&file)?;
            ensure!(meta.len() <= 64 * 1024, "CORS file too large");
            *document = Some(serde_json::from_slice(&std::fs::read(file)?)?);
        }
        _ => {}
    }
    let result = rpc(path, &command).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
fn confirm(identity: &str) -> Result<()> {
    eprintln!("Type the exact target identity to continue: {identity}");
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    ensure!(
        answer.trim_end_matches(['\r', '\n']) == identity,
        "confirmation did not match"
    );
    eprintln!("This operation permanently deletes data. Type DELETE to confirm:");
    answer.clear();
    std::io::stdin().read_line(&mut answer)?;
    ensure!(answer.trim() == "DELETE", "confirmation did not match");
    Ok(())
}
async fn rpc(path: &Path, command: &Command) -> Result<Value> {
    let mut socket = UnixStream::connect(path)
        .await
        .context("cannot reach gateway admin socket; is the server running?")?;
    send_frame(&mut socket, command).await?;
    let reply: Value = serde_json::from_slice(
        &tokio::time::timeout(Duration::from_secs(60), receive_frame(&mut socket)).await??,
    )?;
    if reply["ok"] != true {
        anyhow::bail!(
            "{}",
            reply["error"].as_str().unwrap_or("admin request failed")
        );
    }
    Ok(reply["result"].clone())
}
pub async fn serve(app: Arc<App>) -> Result<()> {
    let path = &app.config.listen.admin_socket;
    tokio::fs::create_dir_all(
        path.parent()
            .context("admin socket needs a parent directory")?,
    )
    .await?;
    if tokio::fs::try_exists(path).await? {
        tokio::fs::remove_file(path).await?;
    }
    let listener = UnixListener::bind(path)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    let slots = Arc::new(tokio::sync::Semaphore::new(4));
    loop {
        let (mut socket, _) = listener.accept().await?;
        let permit = match slots.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = async {
                let data =
                    tokio::time::timeout(Duration::from_secs(10), receive_frame(&mut socket))
                        .await??;
                let command: Command = serde_json::from_slice(&data)?;
                execute(&app, command).await
            }
            .await;
            let reply = match result {
                Ok(v) => json!({"ok":true,"result":v}),
                Err(e) => json!({"ok":false,"error":e.to_string()}),
            };
            let _ = send_frame(&mut socket, &reply).await;
        });
    }
}
pub async fn password_hash(password: String) -> Result<String> {
    ensure!(
        password.len() >= 12 && password.len() <= 1024,
        "password must contain 12..1024 bytes"
    );
    tokio::task::spawn_blocking(move || {
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
        Command::Status => app.status().await,
        Command::Gc(Gc::Status) => app.gc_status().await,
        Command::Gc(Gc::Pause) => {
            sqlx::query("UPDATE gateway_meta SET gc_paused=true")
                .execute(&app.db)
                .await?;
            app.gc_status().await
        }
        Command::Gc(Gc::Resume) => {
            sqlx::query("UPDATE gateway_meta SET gc_paused=false")
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
            sqlx::query("UPDATE gateway_meta SET maintenance=$1")
                .bind(enabled)
                .execute(&app.db)
                .await?;
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
            app.set_cors(b.id, rules).await
        }
        Command::Credential(Credentials::List) => {
            let rows: Vec<(String, bool)> =
                sqlx::query_as("SELECT access_key,enabled FROM credentials ORDER BY access_key")
                    .fetch_all(&app.db)
                    .await?;
            Ok(json!(
                rows.into_iter()
                    .map(|(access_key, enabled)| json!({"access_key":access_key,"enabled":enabled}))
                    .collect::<Vec<_>>()
            ))
        }
        Command::Credential(Credentials::Create { bucket, read_only }) => {
            let b = app.bucket(&bucket, true).await?;
            let access = format!("MGW{}", &random_secret()?[..24]);
            let secret = random_secret()?;
            let protected = codec::protect(
                secret.as_bytes(),
                &app.secrets.credential_key,
                access.as_bytes(),
            )?;
            let mut tx = app.db.begin().await?;
            sqlx::query("INSERT INTO credentials(access_key,secret_encrypted) VALUES($1,$2)")
                .bind(&access)
                .bind(protected)
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO grants(access_key,bucket_id,writable) VALUES($1,$2,$3)")
                .bind(&access)
                .bind(b.id)
                .bind(!read_only)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(
                json!({"access_key":access,"secret_key":secret,"bucket":bucket,"writable":!read_only}),
            )
        }
        Command::Credential(Credentials::Grant {
            access_key,
            bucket,
            read_only,
        }) => {
            let b = app.bucket(&bucket, true).await?;
            sqlx::query("INSERT INTO grants(access_key,bucket_id,writable) VALUES($1,$2,$3) ON CONFLICT(access_key,bucket_id) DO UPDATE SET writable=excluded.writable").bind(access_key).bind(b.id).bind(!read_only).execute(&app.db).await?;
            Ok(json!({"granted":true}))
        }
        Command::Credential(Credentials::Revoke { access_key, bucket }) => {
            let b = app.bucket(&bucket, true).await?;
            sqlx::query("DELETE FROM grants WHERE access_key=$1 AND bucket_id=$2")
                .bind(access_key)
                .bind(b.id)
                .execute(&app.db)
                .await?;
            Ok(json!({"revoked":true}))
        }
        Command::Credential(Credentials::Disable { access_key }) => {
            let n = sqlx::query("UPDATE credentials SET enabled=false WHERE access_key=$1")
                .bind(access_key)
                .execute(&app.db)
                .await?
                .rows_affected();
            ensure!(n == 1, "credential not found");
            Ok(json!({"disabled":true}))
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
        Command::User(Users::List) => {
            let rows: Vec<(Uuid, String, bool)> =
                sqlx::query_as("SELECT id,username,enabled FROM web_users ORDER BY username")
                    .fetch_all(&app.db)
                    .await?;
            Ok(json!(rows.into_iter().map(|(id,username,enabled)|json!({"id":id,"username":username,"enabled":enabled})).collect::<Vec<_>>()))
        }
        Command::User(Users::Create {
            username, password, ..
        }) => {
            ensure!(
                !username.is_empty()
                    && username.len() <= 64
                    && !username.chars().any(char::is_control),
                "invalid username"
            );
            let hash = password_hash(password.context("missing password")?).await?;
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO web_users(id,username,password_hash) VALUES($1,$2,$3)")
                .bind(id)
                .bind(&username)
                .bind(hash)
                .execute(&app.db)
                .await?;
            Ok(json!({"id":id,"username":username}))
        }
        Command::User(Users::Password {
            username, password, ..
        }) => {
            let hash = password_hash(password.context("missing password")?).await?;
            let mut tx = app.db.begin().await?;
            let id: Uuid = sqlx::query_scalar(
                "UPDATE web_users SET password_hash=$2 WHERE username=$1 RETURNING id",
            )
            .bind(&username)
            .bind(hash)
            .fetch_optional(&mut *tx)
            .await?
            .context("user not found")?;
            sqlx::query("DELETE FROM sessions WHERE user_id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(json!({"updated":username}))
        }
        Command::User(Users::Disable { username }) => {
            let mut tx = app.db.begin().await?;
            let id: Uuid = sqlx::query_scalar(
                "UPDATE web_users SET enabled=false WHERE username=$1 RETURNING id",
            )
            .bind(&username)
            .fetch_optional(&mut *tx)
            .await?
            .context("user not found")?;
            sqlx::query("DELETE FROM sessions WHERE user_id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(json!({"disabled":username}))
        }
        Command::User(Users::Delete { username }) => {
            sqlx::query("DELETE FROM web_users WHERE username=$1")
                .bind(&username)
                .execute(&app.db)
                .await?;
            Ok(json!({"deleted":username}))
        }
    }
}
