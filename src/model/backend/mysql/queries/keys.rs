use super::*;

pub(in super::super) fn managed_keys() -> Rows<'static, ManagedKeyMaterial> {
    sqlx::query_as(
        "SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE secret_encrypted IS NOT NULL ORDER BY id",
    )
}

pub(in super::super) fn required_keys() -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT DISTINCT block_key_id FROM (SELECT block_key_id FROM chunk_encodings WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM packs WHERE encryption_algorithm<>'none' AND state<>'deleted' UNION SELECT block_key_id FROM chunks WHERE encryption_algorithm<>'none' AND EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=chunks.id)) sources WHERE block_key_id IS NOT NULL",
    )
}

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
        Ok(sqlx::query_as("SELECT k.id,k.name,c.reserved_id,p.reserved_id,(SELECT count(*) FROM storages s WHERE s.block_key_id=k.id),EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state='uploading') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state='preparing') OR EXISTS(SELECT 1 FROM chunks c WHERE c.block_key_id=k.id AND c.state IN ('preparing','uploading')) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE UNHEX(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(t.detail,'$.target_key_id')),'-',''))=k.id AND t.state IN ('queued','running','paused','failed')),o.data,o.revision FROM managed_keys k LEFT JOIN nonce_counters c ON c.block_key_id=k.id AND c.type_tag=0 LEFT JOIN nonce_counters p ON p.block_key_id=k.id AND p.type_tag=1 LEFT JOIN nonce_observations o ON o.block_key_id=k.id WHERE k.purpose='block' AND k.revoked_at IS NULL ORDER BY k.id").fetch_all(&self.pool).await?)
    }
    async fn save_nonce_history(
        &self,
        key: Uuid,
        previous: Option<i64>,
        history: crate::model::key_usage::NonceHistory,
    ) -> Result<()> {
        if let Some(revision) = previous {
            sqlx::query("UPDATE nonce_observations SET data=?,revision=revision+1 WHERE block_key_id=? AND revision=?").bind(sqlx::types::Json(history)).bind(key).bind(revision).execute(&self.pool).await?;
        } else {
            sqlx::query("INSERT INTO nonce_observations(block_key_id,data,revision) VALUES(?,?,1) ON DUPLICATE KEY UPDATE block_key_id=block_key_id").bind(key).bind(sqlx::types::Json(history)).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)> {
        transactions::reserve_nonce(&self.pool, key, type_tag, units).await
    }

    async fn resolve_block(&self, name: &str) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT id,name,key_algorithm AS algorithm FROM managed_keys WHERE name=? AND purpose='block' AND revoked_at IS NULL").bind(name).fetch_optional(&self.pool).await?)
    }
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>> {
        Ok(sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL").bind(id).fetch_optional(&self.pool).await?)
    }
    async fn for_storage(&self, storage: Uuid) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND k.purpose='block' AND k.revoked_at IS NULL").bind(storage).fetch_optional(&self.pool).await?)
    }
    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>> {
        Ok(sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=? AND secret_encrypted IS NOT NULL").bind(name).fetch_optional(&self.pool).await?)
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

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ManagedKeyView {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            nonce_capacity: None,
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("algorithm")?,
            in_use: row.try_get("in_use")?,
            fingerprint: row.try_get("fingerprint")?,
            created_at: row.try_get("created_at")?,
            revoked_at: row.try_get("revoked_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ManagedKeyRecord {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            algorithm: row.try_get("algorithm")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ManagedKeyMaterial {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("key_algorithm")?,
            encrypted: row.try_get("secret_encrypted")?,
            fingerprint: row.try_get("fingerprint")?,
        })
    }
}
