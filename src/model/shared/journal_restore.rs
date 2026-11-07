use super::{Backend, queries::journal_restore as q};
use crate::{
    authorization::Principal,
    model::{
        Id,
        journal::{
            record::{BlockKey, Change, Material},
            restore::{Job, Location, Progress, RecoveryStore, Selection, key_hash, nonce_id},
        },
    },
    storage::marker::Journal,
};
use anyhow::{Context, Result, ensure};
use uuid::Uuid;

#[async_trait::async_trait]
impl RecoveryStore for Backend {
    async fn recovery_create(
        &self,
        principal: &Principal,
        id: Uuid,
        storage: Uuid,
        source: &[u8],
        progress: &Progress,
    ) -> Result<()> {
        let mut tx = q::admin(self, principal).await?;
        q::create(id, storage, source, progress)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery(&self, id: Uuid) -> Result<Job> {
        Ok(q::job(id).fetch_one(q::read(self)).await?)
    }
    async fn recovery_protects(&self, storage: Uuid) -> Result<bool> {
        Ok(q::protecting(storage).fetch_one(q::read(self)).await?)
    }
    async fn recovery_recent(&self) -> Result<Vec<Uuid>> {
        Ok(q::recent().fetch_all(q::read(self)).await?)
    }
    async fn expire_recovery_previews(&self) -> Result<()> {
        q::expire().execute(q::write(self)).await?;
        Ok(())
    }
    async fn recovery_jobs(&self) -> Result<Vec<Uuid>> {
        Ok(q::pending().fetch_all(q::read(self)).await?)
    }
    async fn recovery_queue(
        &self,
        principal: &Principal,
        id: Uuid,
        revision: i64,
        selection: &Selection,
        source: &[u8],
        target: Uuid,
    ) -> Result<()> {
        let mut tx = q::admin(self, principal).await?;
        ensure!(
            q::queue(selection, source, target, id, revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            "recovery selection changed"
        );
        q::scope(target, id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_control(
        &self,
        principal: &Principal,
        id: Uuid,
        revision: i64,
        retry: bool,
    ) -> Result<()> {
        let mut tx = q::admin(self, principal).await?;
        let op = if retry {
            q::retry(id, revision)
        } else {
            q::cancel(id, revision)
        };
        ensure!(
            op.execute(&mut *tx).await?.rows_affected() == 1,
            "recovery state changed"
        );
        if !retry {
            q::release_scope(id).execute(&mut *tx).await?;
            q::discard_items(id).execute(&mut *tx).await?;
            q::discard_buckets(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_intent(&self, id: Uuid, token: Uuid, source: &[u8]) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(id, token).fetch_one(&mut *tx).await?;
        q::intent(source, id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_claim(&self, id: Uuid, token: Uuid) -> Result<bool> {
        Ok(q::claim(token, id, token)
            .execute(q::write(self))
            .await?
            .rows_affected()
            == 1)
    }
    async fn recovery_save(
        &self,
        id: Uuid,
        token: Uuid,
        state: &str,
        progress: &Progress,
        error: Option<&str>,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(id, token).fetch_one(&mut *tx).await?;
        q::save(state, progress, error, id)
            .execute(&mut *tx)
            .await?;
        if state == "completed" {
            q::release_source(id).execute(&mut *tx).await?;
            q::release_scope(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_key_conflict(&self, key: &BlockKey) -> Result<bool> {
        let fp = *blake3::hash(&key.material).as_bytes();
        Ok(q::conflict(key.id, &fp, &key.algorithm, key.id, &fp)
            .fetch_one(q::read(self))
            .await?)
    }
    async fn recovery_key(
        &self,
        job: Uuid,
        token: Uuid,
        key: &BlockKey,
        protected: &[u8],
    ) -> Result<()> {
        let fp = *blake3::hash(&key.material).as_bytes();
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        ensure!(
            !q::conflict(key.id, &fp, &key.algorithm, key.id, &fp)
                .fetch_one(&mut *tx)
                .await?,
            "historical block key conflicts with an existing key"
        );
        if q::has_key(key.id).fetch_optional(&mut *tx).await?.is_none() {
            q::insert_key(
                key.id,
                &format!("restore-{}", key.id.simple()),
                &key.algorithm,
                protected,
                &fp,
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_material(
        &self,
        job: Uuid,
        token: Uuid,
        target: Uuid,
        value: &Material,
        pack: Option<(Id, i32, i64)>,
    ) -> Result<Id> {
        let hash = key_hash(value.reference())?;
        let fingerprint = *blake3::hash(&serde_json::to_vec(value)?).as_bytes();
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        if let Some((id, old)) = q::mapping(job, &hash).fetch_optional(&mut *tx).await? {
            ensure!(old == fingerprint, "recovered material identity changed");
            return Ok(id);
        }
        let id = match value {
            Material::Chunk { chunk } | Material::Member { chunk, .. } => {
                let packed = matches!(value, Material::Member { .. });
                let pack_id = pack.map(|p| p.0);
                ensure!(
                    packed == pack_id.is_some(),
                    "invalid restored pack membership"
                );
                let existing = q::chunk_by_hash(target, &chunk.hash, chunk.raw_size)
                    .fetch_optional(&mut *tx)
                    .await?;
                if let Some(id) = existing {
                    id
                } else {
                    q::insert_chunk(
                        chunk.uuid,
                        target,
                        &chunk.hash,
                        chunk.raw_size,
                        if packed { "none" } else { &chunk.algorithm },
                        if packed { None } else { chunk.block_key_id },
                        pack_id,
                        chunk.created_at,
                    )
                    .execute(&mut *tx)
                    .await?;
                    let id = q::chunk_id(chunk.uuid).fetch_one(&mut *tx).await?;
                    if let Some((pack, ordinal, offset)) = pack {
                        let retired = Uuid::new_v4();
                        q::packed_encoding(retired, id, target)
                            .execute(&mut *tx)
                            .await?;
                        let encoding = q::encoding_id(retired).fetch_one(&mut *tx).await?;
                        q::ready_chunk(encoding, None, false, None, id)
                            .execute(&mut *tx)
                            .await?;

                        q::member(pack, ordinal, id, offset)
                            .execute(&mut *tx)
                            .await?;
                    } else {
                        q::insert_encoding(
                            chunk.location_id,
                            id,
                            target,
                            chunk.stored_size,
                            chunk.compressed,
                            &chunk.algorithm,
                            chunk.block_key_id,
                            chunk.nonce.as_deref(),
                            nonce_id(chunk.nonce.as_deref())?,
                            chunk.created_at,
                        )
                        .execute(&mut *tx)
                        .await?;
                        let encoding = q::encoding_id(chunk.location_id)
                            .fetch_one(&mut *tx)
                            .await?;
                        q::ready_chunk(
                            encoding,
                            chunk.stored_size,
                            chunk.compressed,
                            chunk.nonce.as_deref(),
                            id,
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                    id
                }
            }
            Material::Pack { pack } => {
                q::insert_pack(
                    pack.location_id,
                    target,
                    &pack.algorithm,
                    pack.block_key_id,
                    pack.raw_size,
                    pack.stored_size,
                    pack.compressed,
                    pack.nonce.as_deref(),
                    nonce_id(pack.nonce.as_deref())?,
                    pack.digest.as_deref(),
                    pack.member_count,
                    pack.created_at,
                )
                .execute(&mut *tx)
                .await?;
                q::pack_id(pack.location_id).fetch_one(&mut *tx).await?
            }
            Material::Raw { source } => {
                q::insert_raw(
                    source.location_id,
                    target,
                    source.size,
                    source.digest.as_deref(),
                    target,
                )
                .execute(&mut *tx)
                .await?;
                q::raw_id(source.location_id).fetch_one(&mut *tx).await?
            }
            Material::Fragment {
                id,
                key,
                size,
                hash,
                ..
            } => {
                q::insert_fragment(*id, i32::try_from(*size)?, hash, target, key)
                    .execute(&mut *tx)
                    .await?;
                q::fragment_id(*id).fetch_one(&mut *tx).await?
            }
        };
        let (address, unit) = match value {
            Material::Chunk { chunk } => (chunk.location_id, "chunk"),
            Material::Member { pack, .. } => (*pack, "pack"),
            Material::Pack { pack } => (pack.location_id, "pack"),
            Material::Raw { source } => (source.location_id, "raw"),
            Material::Fragment { id, .. } => (*id, "fragment"),
        };
        q::record_mapping(job, &hash, id, &fingerprint, address, unit)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }
    async fn recovery_bucket(
        &self,
        job: Uuid,
        token: Uuid,
        bucket: Uuid,
        name: &str,
        generation: i64,
        sequence: i64,
        in_place: bool,
    ) -> Result<(Uuid, bool, Option<String>)> {
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        if q::planned(job, bucket)
            .fetch_optional(&mut *tx)
            .await?
            .is_none()
        {
            q::plan(
                job,
                bucket,
                if in_place { bucket } else { Uuid::new_v4() },
                name,
                generation,
                sequence,
            )
            .execute(&mut *tx)
            .await?;
        }
        let result = q::planned(job, bucket).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(result)
    }
    async fn recovery_item(
        &self,
        job: Uuid,
        token: Uuid,
        change: &Change,
        locations: &[Location],
    ) -> Result<()> {
        change.validate()?;
        ensure!(
            change.ranges.len() == locations.len(),
            "missing recovery locations"
        );
        let obj = change
            .attributes
            .as_ref()
            .context("missing recovered object")?;
        let key = change.key.as_deref().context("missing recovered key")?;
        let mime = obj
            .metadata
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("application/octet-stream");
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        q::stage(
            job,
            change.bucket,
            key,
            obj.size,
            &obj.etag,
            &obj.metadata,
            &obj.checksums,
            &obj.payload,
            obj.modified_us,
            mime,
            q::category(key, mime),
        )
        .execute(&mut *tx)
        .await?;
        for (span, loc) in change.ranges.iter().zip(locations) {
            q::range(
                job,
                change.bucket,
                key,
                span.offset,
                span.length,
                span.source_offset,
                loc.kind,
                loc.native,
            )
            .execute(&mut *tx)
            .await?;
        }
        q::advance(key, obj.size, job, change.bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_publish(
        &self,
        job: Uuid,
        token: Uuid,
        source: Uuid,
        project: Uuid,
        target: Uuid,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        let (bucket, done, _) = q::planned(job, source).fetch_one(&mut *tx).await?;
        if done {
            return Ok(());
        }
        let (name, bytes, objects) = q::totals(job, source).fetch_one(&mut *tx).await?;
        q::create_bucket_with_quota(&mut tx, bucket, &name, project, target, bytes, objects)
            .await?;
        q::streams(bucket, job, source).execute(&mut *tx).await?;
        q::extents(bucket, job, source).execute(&mut *tx).await?;
        q::raw_ranges(bucket, job, source).execute(&mut *tx).await?;
        ensure!(
            q::objects(bucket, bucket, job, source)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == u64::try_from(objects)?,
            "recovered object count mismatch"
        );
        if source == bucket {
            q::seed_recovered(target, job, source)
                .execute(&mut *tx)
                .await?;
            super::queries::journal::set_mode(bucket, 2)
                .execute(&mut *tx)
                .await?;
        }
        q::complete_bucket(job, source).execute(&mut *tx).await?;
        q::remove_items(job, source).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_orphan(
        &self,
        job: Uuid,
        token: Uuid,
        address: Uuid,
        path: &str,
        size: i64,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        q::orphan(job, address, path, size)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn recovery_adopt(
        &self,
        job: Uuid,
        token: Uuid,
        target: Uuid,
        descriptor: &Journal,
    ) -> Result<()> {
        let mut tx = q::begin(self).await?;
        q::fence(job, token).fetch_one(&mut *tx).await?;
        super::queries::journal::ensure_store(target)
            .execute(&mut *tx)
            .await?;
        q::adopt(descriptor, target, descriptor)
            .execute(&mut *tx)
            .await?;
        ensure!(
            super::queries::journal::ledger(target)
                .fetch_one(&mut *tx)
                .await?
                .descriptor
                .as_ref()
                .map(|d| &d.0)
                == Some(descriptor),
            "journal takeover registration conflicts"
        );
        tx.commit().await?;
        Ok(())
    }
}
