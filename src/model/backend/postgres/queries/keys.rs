use super::*;

#[async_trait::async_trait]
impl ManagedKeyStore for Backend {
    async fn replacement_preview(
        &self,
        principal: &Principal,
        source: Uuid,
        input: KeyReplacementInput,
    ) -> Result<KeyReplacementPreview> {
        transactions::key_replacement_preview(&self.pool, principal, source, input).await
    }
    async fn replacement(&self, principal: &Principal, id: Uuid) -> Result<ReplacementRow> {
        transactions::key_replacement(&self.pool, principal, id).await
    }
    async fn replace(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Option<ManagedKeyCreate>,
    ) -> Result<KeyReplacementResult> {
        transactions::replace_keys(&self.pool, principal, id, input).await
    }

    async fn nonce_observations(&self) -> Result<Vec<crate::model::key_usage::NonceRow>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,c.reserved_id,p.reserved_id,(SELECT count(*) FROM storages s WHERE s.block_key_id=k.id),EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state='uploading') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state='preparing') OR EXISTS(SELECT 1 FROM chunks c WHERE c.block_key_id=k.id AND c.state IN ('preparing','uploading')) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE t.detail->>'target_key_id'=k.id::text AND t.state IN ('queued','running','paused','failed')),o.data,o.revision FROM managed_keys k LEFT JOIN nonce_counters c ON c.block_key_id=k.id AND c.type_tag=0 LEFT JOIN nonce_counters p ON p.block_key_id=k.id AND p.type_tag=1 LEFT JOIN nonce_observations o ON o.block_key_id=k.id WHERE k.purpose='block' AND NOT k.recovery_read_only AND k.revoked_at IS NULL ORDER BY k.id").fetch_all(&self.pool).await?)
    }
    async fn save_nonce_history(
        &self,
        key: Uuid,
        previous: Option<i64>,
        history: crate::model::key_usage::NonceHistory,
    ) -> Result<()> {
        sqlx::query("INSERT INTO nonce_observations(block_key_id,data,revision) VALUES($1,$2,1) ON CONFLICT(block_key_id) DO UPDATE SET data=EXCLUDED.data,revision=nonce_observations.revision+1 WHERE nonce_observations.revision=$3").bind(key).bind(sqlx::types::Json(history)).bind(previous.unwrap_or(0)).execute(&self.pool).await?;
        Ok(())
    }

    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)> {
        transactions::reserve_nonce(&self.pool, key, type_tag, units).await
    }

    async fn resolve_block(&self, name: &str) -> Result<Option<crate::model::ManagedKeyRecord>> {
        Ok(resolve_block_key(name)
            .fetch_optional(&self.pool)
            .await?
            .map(|(id, name, algorithm)| crate::model::ManagedKeyRecord {
                id,
                name,
                algorithm,
            }))
    }
    async fn read_block_name(&self, id: Uuid) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT name FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>> {
        Ok(active_block_key(id).fetch_optional(&self.pool).await?)
    }

    async fn for_storage(&self, storage_id: Uuid) -> Result<Option<ManagedKeyRecord>> {
        Ok(storage_key(storage_id).fetch_optional(&self.pool).await?)
    }

    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>> {
        Ok(managed_key(name).fetch_optional(&self.pool).await?)
    }
    async fn list(&self, principal: &Principal) -> Result<Vec<ManagedKeyView>> {
        transactions::list_managed_keys(&self.pool, principal).await
    }
    async fn create(
        &self,
        principal: &Principal,
        input: ManagedKeyCreate,
    ) -> Result<ManagedKeyView> {
        transactions::create_managed_key(&self.pool, principal, input).await
    }
    async fn revoke(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_managed_key(&self.pool, principal, id).await
    }
}

pub(in super::super) fn replacement_storages(source: Uuid) -> Rows<'static, KeyReplacementStorage> {
    sqlx::query_as("SELECT s.id,s.name,s.enabled,p.revision FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.block_key_id=$1 ORDER BY s.id FOR UPDATE OF s,p").bind(source)
}

pub(in super::super) fn replacement_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND NOT recovery_read_only AND revoked_at IS NULL FOR UPDATE").bind(id)
}

pub(in super::super) fn replacement_capacity(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT count(*)=2 AND min(reserved_id)>=0 AND max(reserved_id)<=9223372036854775743 FROM nonce_counters WHERE block_key_id=$1").bind(id)
}

pub(in super::super) fn insert_key_replacement<'q>(
    actor: &'q str,
    plan: &'q KeyReplacementPreview,
) -> Statement<'q> {
    sqlx::query("INSERT INTO key_replacements(id,actor,plan,expires_at) VALUES($1,$2,$3,$4)")
        .bind(plan.id)
        .bind(actor)
        .bind(sqlx::types::Json(plan))
        .bind(plan.expires_at)
}

pub(in super::super) fn key_replacement<'q>(id: Uuid, actor: &'q str) -> Rows<'q, ReplacementRow> {
    sqlx::query_as("SELECT plan,result FROM key_replacements WHERE id=$1 AND actor=$2 FOR UPDATE")
        .bind(id)
        .bind(actor)
}

pub(in super::super) fn complete_key_replacement(result: &KeyReplacementResult) -> Statement<'_> {
    sqlx::query("UPDATE key_replacements SET result=$1,expires_at=clock_timestamp()+interval '7 days' WHERE id=$2 AND result IS NULL").bind(sqlx::types::Json(result)).bind(result.preview.id)
}

pub(in super::super) fn raw_receiving(source: Id, stream: Id) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM raw_sources WHERE id=$1 AND owner_stream=$2 AND state='writing' AND medium='shared_s3' AND upload_id IS NULL FOR UPDATE").bind(source).bind(stream)
}

pub(in super::super) fn raw_discard(source: Id, stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM raw_sources WHERE id=$1 AND owner_stream=$2 AND state='writing' AND upload_id IS NULL AND NOT EXISTS(SELECT 1 FROM node_read_sources n JOIN node_read_windows w ON w.id=n.window_id JOIN stream_leases l ON l.id=w.read_id WHERE n.kind='raw' AND n.location_id=raw_sources.id AND l.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM raw_ranges WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM multipart_files WHERE source_id=raw_sources.id) AND NOT EXISTS(SELECT 1 FROM uploads WHERE raw_source_id=raw_sources.id)").bind(source).bind(stream)
}

pub(in super::super) fn raw_reusable<'a>(
    storage: Uuid,
    hash: &'a [u8],
    size: i64,
    digest: &'a [u8],
) -> Scalar<'a, Id> {
    sqlx::query_scalar("SELECT id FROM raw_sources WHERE storage_id=$1 AND content_hash=$2 AND size=$3 AND digest=$4 AND state='ready' AND medium='shared_s3' AND NOT pending_upload AND quarantine_code IS NULL").bind(storage).bind(hash).bind(size).bind(digest)
}

pub(in super::super) fn raw_uploads(after: Id, owner: Option<Uuid>) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.source_id>$1 AND u.node_id IS NOT DISTINCT FROM $2 ORDER BY u.source_id LIMIT 128").bind(after).bind(owner)
}

pub(in super::super) fn raw_upload_record(source: Id) -> Rows<'static, RawUpload> {
    sqlx::query_as("SELECT r.id AS raw_id,r.id AS raw_location,r.storage_id AS raw_storage,r.size AS raw_size,r.digest AS raw_digest,r.state AS raw_state,r.medium AS raw_medium,r.pending_upload AS raw_pending,r.revision AS raw_revision,r.quarantine_code AS raw_quarantine,u.token,u.deduplicate,u.background,u.content_hash FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE u.source_id=$1 FOR UPDATE").bind(source)
}

pub(in super::super) fn raw_upload_due(
    before: Option<chrono::DateTime<chrono::Utc>>,
    owner: Option<Uuid>,
    urgent: &[Uuid],
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::new(
        "SELECT u.source_id FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id JOIN storage_profiles sp ON sp.storage_id=r.storage_id WHERE u.next_attempt_at<=clock_timestamp() AND r.state='ready'",
    );
    query
        .push(" AND u.node_id IS NOT DISTINCT FROM ")
        .push_bind(owner)
        .push(" AND (")
        .push_bind(owner)
        .push("::uuid IS NULL OR u.token IS NULL OR u.lease_expires_at<clock_timestamp())");
    if let Some(before) = before {
        query.push(" AND r.created_at<=").push_bind(before);
    } else {
        query.push(" AND (NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=r.processing_task AND t.state<>'completed') OR r.created_at+sp.write_backlog_max_age*interval '1 second'<=clock_timestamp())");
        write_delay(&mut query, "r.created_at", "r.storage_id", urgent);
    }
    query.push(" ORDER BY CASE WHEN r.created_at+(sp.write_backlog_max_age)*interval '1 second'<=clock_timestamp() THEN 0 ELSE 1 END,u.background,u.next_attempt_at,u.source_id LIMIT 1 FOR UPDATE OF u SKIP LOCKED");
    query
}

pub(in super::super) fn raw_upload_claim(
    source: Id,
    token: Uuid,
    node: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query("UPDATE raw_uploads SET token=$1,node_id=$3,lease_expires_at=CASE WHEN $3::uuid IS NULL THEN NULL ELSE clock_timestamp()+interval '120 seconds' END WHERE source_id=$2").bind(token).bind(source).bind(node)
}

pub(in super::super) fn raw_queue(
    source: Id,
    deduplicate: bool,
    background: bool,
    owner: Option<Uuid>,
) -> Statement<'static> {
    sqlx::query(
        "WITH scoped AS (UPDATE tasks SET execution_scope=CASE WHEN $4::uuid IS NULL THEN 'cluster' ELSE 'node' END,target_node_id=$4 WHERE id=(SELECT processing_task FROM raw_sources WHERE id=$1)) INSERT INTO raw_uploads(source_id,deduplicate,background,node_id) VALUES($1,$2,$3,$4)",
    )
    .bind(source)
    .bind(deduplicate)
    .bind(background)
    .bind(owner)
}
