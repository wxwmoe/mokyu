use super::*;
use crate::{content::codec::Key, model::storage_recovery::Link};
use std::{
    fs,
    process::{Command, Output},
};

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires frozen test media, their owner key and the dump binary"]
fn offline_takeover_fork_and_retirement() -> Result<()> {
    let medium = PathBuf::from(std::env::var("MOKYU_TEST_FROZEN_MEDIA")?);
    let namespace = Uuid::parse_str(&std::env::var("MOKYU_TEST_FROZEN_NAMESPACE")?)?;
    let key_file = std::env::var("MOKYU_DUMP_DEPLOYMENT_KEY_FILE")?;
    let key = Key::new(
        "aes-256-gcm",
        crate::config::file::deployment_key(fs::read_to_string(&key_file)?.trim())?,
    )?;
    let work = tempfile::tempdir()?;
    let disk = work.path().join("disk");
    copy_dir(&medium, &disk)?;
    let old: Manifest = serde_json::from_slice(&fs::read(disk.join("cluster.json"))?)?;
    let head = disk.join(format!("recovery/heads/{}.json", namespace.simple()));
    let link: Link = serde_json::from_slice(&fs::read(&head)?)?;
    let history = disk.join(format!("recovery/history/{}", namespace.simple()));
    let previous: Proof = serde_json::from_slice(&fs::read(
        history.join(format!("{}.json", link.id.simple())),
    )?)?;
    let run = |media: &[&Path], name: &str| -> Result<Output> {
        let sources = media
            .iter()
            .map(|p| format!("{{path={:?}}}", p.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(",");
        let list = work.path().join("sources.toml");
        fs::write(
            &list,
            format!("type='storage_cluster'\nnamespace='{namespace}'\nsources=[{sources}]\n"),
        )?;
        Ok(Command::new(std::env::var("MOKYU_TEST_DUMP_BIN")?)
            .args(["dump", "ls", "mokyu://media/"])
            .env("MOKYU_DUMP_SOURCE", list)
            .env("MOKYU_DUMP_DB", work.path().join(name))
            .env("MOKYU_DUMP_DEPLOYMENT_KEY_FILE", &key_file)
            .output()?)
    };
    let publish = |proof: &Proof| -> Result<()> {
        fs::write(
            history.join(format!("{}.json", proof.publication.id.simple())),
            serde_json::to_vec(proof)?,
        )?;
        fs::write(&head, serde_json::to_vec(&proof.link()?)?)?;
        Ok(())
    };
    let mut takeover = old.clone();
    takeover.previous = Some(old.digest()?);
    takeover.record_id = Uuid::now_v7();
    takeover.revision += 1;
    takeover.takeover_epoch += 1;
    takeover.follows(&old)?;
    let bytes = serde_json::to_vec(&takeover)?;
    fs::write(
        disk.join(format!(
            "cluster/history/{}/{}.json",
            old.cluster_id.simple(),
            takeover.record_id.simple()
        )),
        &bytes,
    )?;
    fs::write(disk.join("cluster.json"), bytes)?;
    let mut update = previous.publication.clone();
    update.previous = Some(link);
    update.revision += 1;
    update.id = Uuid::now_v7();
    update.manifest = takeover.digest()?;
    update.takeover_epoch = takeover.takeover_epoch;
    update.parts.clear();
    update.retired.clear();
    let accepted = Proof::sign(update.clone(), &key, &takeover)?;
    publish(&accepted)?;
    let result = run(&[&medium, &disk], "incremental.db")?;
    ensure!(
        result.status.success() && String::from_utf8_lossy(&result.stdout).contains("packed.bin"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let fork = work.path().join("fork");
    copy_dir(&disk, &fork)?;
    update.id = Uuid::now_v7();
    let competing = Proof::sign(update.clone(), &key, &takeover)?;
    publish(&competing)?;
    let result = run(&[&disk, &fork], "fork.db")?;
    ensure!(
        !result.status.success() && !work.path().join("fork.db").exists(),
        "signed competing heads were merged"
    );

    let required = previous
        .publication
        .parts
        .iter()
        .find(|p| p.path.contains("/changes/"))
        .context("fixture needs a published change segment")?;
    update.id = Uuid::now_v7();
    update.previous = Some(competing.link()?);
    update.revision += 1;
    update.retired = vec![required.intent.operation];
    publish(&Proof::sign(update, &key, &takeover)?)?;
    let result = run(&[&disk], "retired.db")?;
    ensure!(
        !result.status.success() && !work.path().join("retired.db").exists(),
        "a retired journal segment was reused"
    );
    Ok(())
}
