use crate::model::Id;
use crate::model::task::Execution;
use crate::{app::App, model::packing::RewriteTask};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use uuid::Uuid;

impl App {
    pub async fn rewrite_start(
        &self,
        storage_id: Uuid,
        target_key: Option<&str>,
        execute: bool,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        let target = if let Some(name) = target_key {
            let row = self
                .managed_keys
                .resolve_block(name)
                .await?
                .context("active block key was not found")?;
            Some((row.id, row.name, row.algorithm))
        } else {
            None
        };
        if let Some((_, name, _)) = target.as_ref() {
            self.storage.ensure_key_loaded_name(name).await?;
        }
        let target_algorithm = target
            .as_ref()
            .map(|(_, _, algorithm)| algorithm.as_str())
            .unwrap_or("none");
        let target_name = target
            .as_ref()
            .map(|(_, name, _)| name.as_str())
            .unwrap_or("");
        let profile = self.storage_profiles.load(storage_id).await?;
        let (loose, packed) = self
            .packing
            .rewrite_counts(storage_id, target_algorithm, target.as_ref().map(|v| v.0))
            .await?;
        let preview = json!({"storage_id":storage_id,"target_key":target_name,"target_algorithm":target_algorithm,"compression_strategy":profile.compression_strategy,"loose_chunks":loose,"packed_chunks":packed,"effect":"rewrite physical encodings and retain old sources until GC"});
        if !execute {
            let confirmation = blake3::hash(&serde_json::to_vec(&preview)?)
                .to_hex()
                .to_string();
            return Ok(json!({"preview":preview,"confirmation":confirmation}));
        }
        if confirmation
            != Some(
                blake3::hash(&serde_json::to_vec(&preview)?)
                    .to_hex()
                    .as_str(),
            )
        {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
        let shards = if loose + packed == 0 {
            0
        } else if self.role == "single" {
            1
        } else {
            (loose + packed).clamp(1, 8)
        };
        let job = RewriteTask {
            storage_id,
            upper_chunk_id: Id::default(),
            shard: 0,
            shards,
            profile_revision: profile.revision,
            target_key_id: target.as_ref().map(|v| v.0),
            target_key: target_name.to_owned(),
            target_algorithm: target_algorithm.to_owned(),
        };
        let _coord = self.coord.lock().await;
        self.writable()?;
        let (id, existing) = self
            .packing
            .start_rewrite(&job, self.node.get().map(|n| n.node_id))
            .await?;
        if existing {
            return Ok(json!({"task_id":id,"existing":true}));
        }
        self.wake_tasks.notify_one();
        let mut result = json!({"task_id":id,"preview":preview});
        if self.role != "single" {
            result["children"] = json!(shards);
        }
        Ok(result)
    }

    pub(super) async fn rewrite_batch(
        &self,
        id: Uuid,
        token: Uuid,
        cursor: Option<&str>,
        detail: Value,
    ) -> Result<bool> {
        let execution = Execution { id, token };
        let job: RewriteTask = serde_json::from_value(detail)?;
        let after = cursor.unwrap_or("0").parse::<Id>()?;
        let profile = self.storage_profiles.load(job.storage_id).await?;
        let _storage_permit = self
            .storage
            .profile_permit(job.storage_id, "maintenance")
            .await?;
        let strategy = match profile.compression_strategy.as_str() {
            "never" => crate::content::compression::Strategy::Never,
            "sample" => crate::content::compression::Strategy::Sample,
            _ => crate::content::compression::Strategy::Always,
        };
        ensure!(
            job.profile_revision == 0 || job.profile_revision == profile.revision,
            "storage profile changed during rewrite; preview again"
        );
        if let Some(key_id) = job.target_key_id {
            let current = self.managed_keys.active_block(key_id).await?;
            ensure!(
                current.as_ref().is_some_and(|(name, algorithm)| {
                    name == &job.target_key && algorithm == &job.target_algorithm
                }),
                "rewrite target key was removed or changed"
            );
            self.storage.ensure_key_loaded_name(&job.target_key).await?;
        }
        let (row, packed_candidate) = self.packing.rewrite_candidates(after, &job).await?;
        let process_loose = row.as_ref().is_some_and(|source| {
            packed_candidate
                .as_ref()
                .is_none_or(|(first_id, _)| source.id < *first_id)
        });
        if process_loose {
            let source = row.expect("rewrite loose candidate disappeared");
            self.pin_pack_inputs(
                execution,
                &[crate::worker::packing::Mapping {
                    chunk: source.clone(),
                    offset_bytes: 0,
                    pack_id: None,
                }],
            )
            .await?;
            let raw = self.storage.get_with(&source, None).await?;
            let mut physical = source.clone();
            physical.location_id = if matches!(physical.encoding_id, Id::Integer(_)) {
                Uuid::now_v7()
            } else {
                Uuid::nil()
            };
            physical.algorithm = job.target_algorithm.clone();
            physical.block_key_id = job.target_key_id;
            physical.created_at = chrono::Utc::now();
            physical.nonce = None;
            physical.encoding_id = self.packing.prepare_encoding(&physical, execution).await?;
            if let Id::Uuid(id) = physical.encoding_id {
                physical.location_id = id;
            }
            let (physical, encoded, _) = self
                .storage
                .encode(physical, raw.to_vec(), source.compressed, None, None)
                .await?;
            self.storage
                .put_path_for(
                    job.storage_id,
                    &self
                        .storage
                        .path_for(job.storage_id, physical.location_id)?,
                    encoded,
                )
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("rewrite-chunk-stored").await;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            self.packing
                .commit_rewrite_chunk(id, token, &source, &physical)
                .await?;
            return Ok(false);
        }

        if let Some((first_id, pack_id)) = packed_candidate {
            let (old, members, current_members) = self.packing.rewrite_pack_inputs(pack_id).await?;
            let mappings: Vec<_> = members
                .iter()
                .filter(|c| current_members.contains(&c.id))
                .map(|c| crate::worker::packing::Mapping {
                    chunk: c.clone(),
                    offset_bytes: 0,
                    pack_id: Some(old.id),
                })
                .collect();
            self.pin_pack_inputs(execution, &mappings).await?;
            let work = self.storage.pack_work(old.payload_size()?).await?;
            let loaded = self.storage.load_pack(old.clone()).await?;
            let mut encoded_members = Vec::with_capacity(members.len());
            for mut member in members {
                let raw = loaded.data.chunk(&member)?;
                member.algorithm = job.target_algorithm.clone();
                member.block_key_id = job.target_key_id;
                encoded_members.push((member, raw));
            }
            let new_pack = self
                .packing
                .prepare_pack(
                    job.storage_id,
                    &job.target_algorithm,
                    job.target_key_id,
                    old.raw_size,
                    old.member_count,
                    execution,
                )
                .await?;
            let (new_pack, encoded) = self
                .storage
                .encode_pack(
                    new_pack,
                    encoded_members.clone(),
                    strategy,
                    profile.compression_enabled
                        && strategy != crate::content::compression::Strategy::Never,
                    &work,
                    None,
                )
                .await?;
            self.storage
                .put_path_for(
                    job.storage_id,
                    &self
                        .storage
                        .pack_path_for(job.storage_id, new_pack.location_id)?,
                    encoded,
                )
                .await?;
            #[cfg(feature = "fault-injection")]
            crate::faults::point("rewrite-pack-stored").await;
            self.storage_writable()?;
            let _gate = self.storage.source_gate.write().await;
            let members = encoded_members
                .into_iter()
                .map(|(chunk, _)| chunk)
                .collect::<Vec<_>>();
            self.packing
                .commit_rewrite_pack(
                    id,
                    token,
                    &old,
                    &new_pack,
                    &current_members,
                    &members,
                    first_id,
                )
                .await?;
            return Ok(false);
        }

        self.packing.complete_rewrite(id, token, after).await?;
        Ok(false)
    }
}
