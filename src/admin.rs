use crate::app::App;
use crate::control::operations::execute;
use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{IsTerminal, Read},
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
    #[command(name = "cred")]
    #[command(subcommand)]
    Credential(Credentials),
    #[command(subcommand)]
    Token(Tokens),
    Audit {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        action: Option<String>,
    },
    #[command(subcommand)]
    Domain(Domains),
    #[command(subcommand)]
    User(Users),
    #[command(subcommand)]
    Project(Projects),
    #[command(subcommand)]
    Quota(Quotas),
    #[command(subcommand)]
    Gc(Gc),
    #[command(subcommand)]
    Cleanup(Cleanup),
    #[command(subcommand)]
    Maintenance(Maintenance),
    #[command(subcommand)]
    Task(Tasks),
    #[command(name = "storage")]
    #[command(subcommand)]
    Stores(Stores),
    #[command(subcommand)]
    Settings(Settings),
    #[command(name = "key")]
    #[command(subcommand)]
    Keys(Keys),
    #[command(subcommand)]
    Cluster(Cluster),
    #[command(subcommand)]
    Setup(Setup),
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
        #[arg(long = "mode", default_value = "pack")]
        mode: String,
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
        #[arg(long)]
        project: Option<Uuid>,
        #[arg(long)]
        store: Option<Uuid>,
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
    MarkUnreferenced,
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
    Status,
    Pause {
        task: String,
    },
    Resume {
        task: String,
    },
    Run {
        task: String,
    },
    PackCreation {
        #[arg(value_parser=["stop","resume"])]
        action: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Tasks {
    List,
    Show { id: Uuid },
    Pause { id: Uuid },
    Resume { id: Uuid },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Stores {
    List,
    Create {
        name: String,
        #[arg(value_parser = ["local", "s3"])]
        r#type: String,
        #[arg(long = "init")]
        initialize: bool,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(short = 'e', long)]
        endpoint: Option<String>,
        #[arg(short = 'b', long)]
        bucket: Option<String>,
        #[arg(short = 'r', long)]
        region: Option<String>,
        #[arg(short = 'p', long, default_value = "")]
        prefix: String,
        #[arg(short = 'a', long = "access")]
        access_key: Option<String>,
        #[arg(short = 's', long = "secret")]
        secret_key: Option<String>,
        #[arg(long = "read")]
        read_concurrency: Option<i32>,
        #[arg(long = "upload")]
        upload_concurrency: Option<i32>,
        #[arg(long = "control")]
        control_concurrency: Option<i32>,
    },
    Update {
        name: String,
        #[arg(long, value_parser = ["local", "s3"])]
        r#type: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        key: Option<String>,
        #[arg(short = 'e', long)]
        endpoint: Option<String>,
        #[arg(short = 'b', long)]
        bucket: Option<String>,
        #[arg(short = 'r', long)]
        region: Option<String>,
        #[arg(short = 'p', long)]
        prefix: Option<String>,
        #[arg(short = 'a', long = "access")]
        access_key: Option<String>,
        #[arg(short = 's', long = "secret")]
        secret_key: Option<String>,
        #[arg(long)]
        enabled: Option<bool>,
        #[arg(long = "read")]
        read_concurrency: Option<i32>,
        #[arg(long)]
        clear_read_concurrency: bool,
        #[arg(long = "upload")]
        upload_concurrency: Option<i32>,
        #[arg(long)]
        clear_upload_concurrency: bool,
        #[arg(long = "control")]
        control_concurrency: Option<i32>,
        #[arg(long)]
        clear_control_concurrency: bool,
    },
    Check {
        name: String,
    },
    Show {
        name: String,
    },
    Key {
        #[command(subcommand)]
        command: StoreKey,
    },
    #[command(name = "rewrite")]
    Rekey {
        store: String,
        #[command(subcommand)]
        command: RekeyCommand,
    },
    Sweep {
        #[arg(long)]
        store: Option<Uuid>,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        preview: Option<Uuid>,
        #[arg(skip)]
        confirm_prefix: Option<String>,
        #[arg(long = "age", default_value = "48h")]
        older_than: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum StoreKey {
    Set { store: String, key: String },
    Clear { store: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum RekeyCommand {
    Preview {
        #[arg(long = "key")]
        target_key: Option<String>,
    },
    Start {
        #[arg(long = "key")]
        target_key: Option<String>,
        #[arg(long)]
        confirm: String,
    },
    Status,
    Pause,
    Resume,
    Retry,
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Settings {
    Get {
        section: String,
    },
    Set {
        section: String,
        file: PathBuf,
        #[arg(long = "rev")]
        revision: i64,
        #[arg(skip)]
        document: Option<Value>,
    },
    Reset {
        section: String,
    },
    #[command(subcommand)]
    #[command(name = "web")]
    Control(ControlSettings),
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum ControlSettings {
    Show,
    Set {
        #[arg(long)]
        origin: Option<String>,
        #[arg(long)]
        secure_cookie: Option<bool>,
        #[arg(long)]
        session_lifetime: Option<String>,
        #[arg(long)]
        gravatar_base_url: Option<String>,
        #[arg(long = "rev")]
        revision: Option<i64>,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Keys {
    List,
    Create {
        name: String,
        #[arg(value_parser = ["access", "signing", "block"])]
        purpose: String,
        #[arg(short = 'a', long, value_parser = ["aes-256-gcm", "chacha20-poly1305"])]
        algorithm: Option<String>,
    },
    Revoke {
        id: Uuid,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Cluster {
    Status,
    Mode {
        #[arg(value_parser = ["single", "cluster"])]
        mode: String,
    },
    Nodes,
    Token {
        name: String,
        #[arg(value_parser = ["control", "worker", "data"])]
        kind: String,
        #[arg(long)]
        advertise_url: Option<String>,
        #[arg(long, default_value = "1h")]
        expires_in: String,
    },
    Revoke {
        id: Uuid,
    },
    Reissue {
        id: Uuid,
        #[arg(long, default_value = "1h")]
        expires_in: String,
    },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Setup {
    Token,
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
        #[arg(long = "ttl")]
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
        #[arg(long = "ttl")]
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
impl Command {
    pub fn audit_action(&self) -> Option<&'static str> {
        match self {
            Self::Status
            | Self::Audit { .. }
            | Self::Bucket(Buckets::List)
            | Self::Credential(Credentials::List { .. } | Credentials::Show { .. })
            | Self::Token(Tokens::List { .. })
            | Self::Domain(Domains::List)
            | Self::User(Users::List)
            | Self::Project(Projects::List)
            | Self::Quota(Quotas::Show { .. })
            | Self::Gc(Gc::Status)
            | Self::Cleanup(Cleanup::Status)
            | Self::Maintenance(Maintenance::Status)
            | Self::Task(Tasks::List | Tasks::Show { .. })
            | Self::Integrity(Integrity::Issues { .. })
            | Self::Pack(Packs::Status)
            | Self::Cache(Cache::Status)
            | Self::Stores(Stores::List | Stores::Show { .. })
            | Self::Settings(Settings::Control(ControlSettings::Show) | Settings::Get { .. })
            | Self::Keys(Keys::List)
            | Self::Setup(Setup::Token)
            | Self::Cluster(Cluster::Status | Cluster::Nodes) => None,
            Self::Bucket(_) => Some("bucket.change"),
            Self::Credential(_) => Some("credential.change"),
            Self::Token(_) => Some("token.change"),
            Self::Domain(_) => Some("domain.change"),
            Self::User(_) => Some("user.change"),
            Self::Project(_) => Some("project.change"),
            Self::Quota(_) => Some("quota.update"),
            Self::Gc(_) => Some("gc.change"),
            Self::Cleanup(_) => Some("cleanup.run"),
            Self::Maintenance(_) => Some("maintenance.change"),
            Self::Task(_) => Some("task.change"),
            Self::Stores(_) => Some("storage.change"),
            Self::Settings(_) => Some("runtime.settings"),
            Self::Keys(_) => Some("managed_key.change"),
            Self::Cluster(_) => Some("cluster.change"),
            Self::Integrity(_) => Some("integrity.check"),
            Self::Pack(_) => Some("pack.change"),
            Self::Cache(_) => Some("cache.flush"),
        }
    }
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Domains {
    List,
    Set { host: String, bucket: String },
    Delete { host: String },
}
#[derive(Subcommand, Serialize, Deserialize)]
pub enum Quotas {
    #[command(name = "get")]
    Show {
        #[arg(value_parser=["project","bucket"])]
        scope: String,
        id: Uuid,
    },
    Set {
        #[arg(value_parser=["project","bucket"])]
        scope: String,
        id: Uuid,
        file: PathBuf,
        #[arg(skip)]
        document: Option<Value>,
    },
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
        #[arg(long = "stdin")]
        password_stdin: bool,
        #[arg(skip)]
        password: Option<String>,
    },
    Password {
        username: String,
        #[arg(long)]
        require_change: bool,
        #[arg(long = "stdin")]
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

pub fn deployment_key() -> Result<String> {
    let mut bytes = [0; 48];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    use base64::Engine;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}
fn read_json(path: &Path, limit: usize, label: &str) -> Result<Value> {
    let bytes = if path == Path::new("-") {
        let mut bytes = Vec::new();
        let mut stdin = std::io::stdin();
        stdin.read_to_end(&mut bytes)?;
        bytes
    } else {
        ensure!(std::fs::metadata(path)?.len() <= limit as u64, "{label} too large");
        std::fs::read(path)?
    };
    ensure!(bytes.len() <= limit, "{label} too large");
    Ok(serde_json::from_slice(&bytes)?)
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
        | Command::Quota(Quotas::Set { file, document, .. })
        | Command::Token(
            Tokens::Create { file, document, .. } | Tokens::Update { file, document, .. },
        )
        | Command::Credential(
            Credentials::Update { file, document, .. }
            | Credentials::Permissions { file, document, .. },
        )
        | Command::Settings(Settings::Set { file, document, .. }) => {
            *document = Some(read_json(file, 512 * 1024, "JSON file")?);
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
        Command::Stores(Stores::Sweep {
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
                    "password input requires a terminal; use docker exec -it or --stdin"
                );
                let text = rpassword::prompt_password("Password: ")?;
                let confirmation = rpassword::prompt_password("Confirm password: ")?;
                ensure!(text == confirmation, "passwords do not match");
                text
            });
        }
        Command::Bucket(Buckets::Cors { file, document, .. }) => {
            *document = Some(read_json(file, 64 * 1024, "CORS file")?);
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
pub async fn serve(app: Arc<App>, management: bool) -> Result<()> {
    let path = &app.config.instance.listen_socket;
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
                ensure!(
                    management || matches!(&command, Command::Status),
                    "this role only exposes status"
                );
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
