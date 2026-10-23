use super::commands::*;
use super::transport::{cli_target, rpc};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    io::{IsTerminal, Read},
    path::Path,
};

fn read_json(path: &Path, limit: usize, label: &str) -> Result<Value> {
    let mut bytes = Vec::new();
    if path == Path::new("-") {
        std::io::stdin()
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(path)?
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
    }
    ensure!(bytes.len() <= limit, "{label} too large");
    Ok(serde_json::from_slice(&bytes)?)
}
pub async fn client(path: &Path, mut command: Command) -> Result<()> {
    match &mut command {
        Command::User(Users::Membership { file, document, .. })
        | Command::Quota(Quotas::Set { file, document, .. })
        | Command::Storages(Storages::Profile {
            command:
                StorageProfiles::Set { file, document, .. }
                | StorageProfiles::Preview { file, document, .. },
        })
        | Command::Token(
            Tokens::Create { file, document, .. } | Tokens::Update { file, document, .. },
        )
        | Command::Credential(
            Credentials::Update { file, document, .. }
            | Credentials::Permissions { file, document, .. },
        )
        | Command::Settings(Settings::Set { file, document, .. }) => {
            *document = Some(read_json(file, 512 * 1024, "JSON file")?)
        }
        Command::Bucket(Buckets::Cors { file, document, .. }) => {
            *document = Some(read_json(file, 64 * 1024, "CORS file")?)
        }
        Command::Init {
            password_stdin,
            password,
            ..
        }
        | Command::User(Users::Create {
            password_stdin,
            password,
            ..
        })
        | Command::User(Users::Password {
            password_stdin,
            password,
            ..
        }) => {
            *password = Some(if *password_stdin {
                let mut text = String::new();
                std::io::stdin().read_line(&mut text)?;
                text.trim_end_matches(['\r', '\n']).to_owned()
            } else {
                ensure!(
                    std::io::stdin().is_terminal(),
                    "password input requires a terminal; use --stdin"
                );
                let text = rpassword::prompt_password("Password: ")?;
                let confirmation = rpassword::prompt_password("Confirm password: ")?;
                ensure!(text == confirmation, "passwords do not match");
                text
            });
        }
        _ => {}
    }
    let target = cli_target(path)?;
    let wait = match &command {
        Command::Cache(Cache::Flush {
            wait: true,
            timeout,
        }) => {
            let seconds = crate::config::seconds(timeout)?;
            ensure!(seconds <= 1800, "cache flush timeout must be 1s..30m");
            Some(std::time::Duration::from_secs(seconds))
        }
        _ => None,
    };
    let result = rpc(&target, &command).await?;
    if let Some(timeout) = wait {
        let id =
            serde_json::from_value(result["task_id"].clone()).context("flush task is missing")?;
        let completed = tokio::time::timeout(timeout, async {
            loop {
                let task = rpc(&target, &Command::Task(Tasks::Show { id })).await?;
                match task["state"].as_str() {
                    Some("completed") => return Ok::<_, anyhow::Error>(task),
                    Some("failed" | "paused") => {
                        anyhow::bail!("cache flush {id} stopped: {}", task["error"])
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
                }
            }
        })
        .await;
        match completed {
            Ok(Ok(task)) => println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({"task_id": id, "state": "completed", "detail": task["detail"]})
                )?
            ),
            Ok(Err(error)) => {
                println!("{}", serde_json::to_string_pretty(&result)?);
                return Err(error);
            }
            Err(_) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"task_id": id, "state": "timeout"})
                    )?
                );
                anyhow::bail!("cache flush {id} timed out; the task continues on the server");
            }
        }
        return Ok(());
    }
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
