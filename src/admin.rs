use crate::app::App;
use crate::manage::operations::execute;
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
    Token(Tokens),
    #[command(subcommand)]
    Domain(Domains),
    #[command(subcommand)]
    User(Users),
    #[command(subcommand)]
    Project(Projects),
    #[command(subcommand)]
    Gc(Gc),
    #[command(subcommand)]
    Cleanup(Cleanup),
    #[command(subcommand)]
    Maintenance(Maintenance),
    #[command(subcommand)]
    Task(Tasks),
    #[command(subcommand)]
    Backend(Backend),
    #[command(subcommand)]
    Integrity(Integrity),
    #[command(subcommand)]
    Pack(Packs),
    #[command(subcommand)]
    Cache(Cache),
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Cache {
    Status,
    Flush,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Packs {
    Status,
    Run {
        #[arg(long, default_value = "pack")]
        kind: String,
    },
    Unpack {
        id: Option<i64>,
        #[arg(long, conflicts_with = "id")]
        all: bool,
        #[arg(long)]
        execute: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Integrity {
    Check {
        #[arg(long, value_enum, default_value = "metadata")]
        mode: crate::integrity::Mode,
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long, requires = "bucket")]
        key: Option<String>,
    },
    Issues {
        id: Uuid,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
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
pub enum Cleanup {
    Status,
    Run,
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
    List {
        #[arg(long)]
        after: Option<String>,
    },
    Create {
        bucket: String,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        expires_in: Option<String>,
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
    Enable {
        access_key: String,
    },
    Delete {
        access_key: String,
    },
    Show {
        access_key: String,
    },
    Rotate {
        access_key: String,
        #[arg(long, default_value = "24h")]
        overlap: String,
        #[arg(long)]
        expires_in: Option<String>,
    },
    Update {
        access_key: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Permissions {
        access_key: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Tokens {
    List {
        username: String,
        #[arg(long)]
        after: Option<Uuid>,
    },
    Create {
        username: String,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Update {
        id: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Revoke {
        id: Uuid,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Domains {
    List,
    Set { host: String, bucket: String },
    Delete { host: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Projects {
    List,
    Create {
        name: String,
    },
    Update {
        id: Uuid,
        name: String,
        #[arg(long, default_value = "")]
        description: String,
        #[arg(long)]
        allow_bucket_create: bool,
    },
    Delete {
        id: Uuid,
    },
    Mode {
        enabled: bool,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Users {
    List,
    Create {
        username: String,
        #[arg(long, default_value="admin", value_parser=["admin","member"])]
        role: String,
        #[arg(long)]
        require_change: bool,
        #[arg(long)]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Password {
        username: String,
        #[arg(long)]
        require_change: bool,
        #[arg(long)]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Disable {
        username: String,
    },
    Enable {
        username: String,
    },
    Role {
        username: String,
        #[arg(value_parser=["admin","member"])]
        role: String,
    },
    Membership {
        username: String,
        project: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
    Leave {
        username: String,
        project: Uuid,
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
        Command::User(Users::Membership { file, document, .. })
        | Command::Token(
            Tokens::Create { file, document, .. } | Tokens::Update { file, document, .. },
        )
        | Command::Credential(
            Credentials::Update { file, document, .. }
            | Credentials::Permissions { file, document, .. },
        ) => {
            ensure!(
                std::fs::metadata(&file)?.len() <= 512 * 1024,
                "JSON file too large"
            );
            *document = Some(serde_json::from_slice(&std::fs::read(file)?)?);
        }
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
            *password = Some(if *password_stdin {
                let mut text = String::new();
                std::io::stdin().read_line(&mut text)?;
                text.trim_end_matches(['\r', '\n']).to_owned()
            } else {
                ensure!(
                    std::io::stdin().is_terminal(),
                    "password input requires a terminal; use docker exec -it or --password-stdin"
                );
                let text = rpassword::prompt_password("Password: ")?;
                let confirmation = rpassword::prompt_password("Confirm password: ")?;
                ensure!(text == confirmation, "passwords do not match");
                text
            });
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
