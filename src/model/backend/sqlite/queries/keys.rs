use super::*;

const KEY_REFERENCES: &str = "EXISTS(SELECT 1 FROM storages s WHERE s.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_locations h WHERE h.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tier_moves m WHERE m.source_key=k.id OR m.target_key=k.id) OR EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=0 AND h.physical_id=e.location_id)) OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.location_id)) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE replace(lower(json_extract(t.detail,'$.target_key_id')),'-','')=lower(hex(k.id)) AND t.state IN ('queued','running','paused','failed'))";

pub(in super::super) async fn managed_key_views(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Vec<ManagedKeyView>> {
    let mut query = managed_key_query();
    query.push(" ORDER BY k.name");
    Ok(query.build_query_as().fetch_all(connection).await?)
}

fn managed_key_query() -> sqlx::QueryBuilder<Sqlite> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT k.id,k.name,k.purpose,k.key_algorithm AS algorithm,");
    query.push(KEY_REFERENCES).push(" AS in_use,LOWER(HEX(k.fingerprint)) AS fingerprint,k.created_at,k.revoked_at FROM managed_keys k");
    query
}

pub(in super::super) async fn managed_key_view(
    connection: &mut sqlx::SqliteConnection,
    id: Uuid,
) -> Result<ManagedKeyView> {
    let mut query = managed_key_query();
    query.push(" WHERE k.id=").push_bind(id);
    Ok(query.build_query_as().fetch_one(connection).await?)
}

pub(in super::super) async fn managed_key_in_use(
    connection: &mut sqlx::SqliteConnection,
    id: Uuid,
) -> Result<bool> {
    let mut query = sqlx::QueryBuilder::new("SELECT ");
    query
        .push(KEY_REFERENCES)
        .push(" FROM managed_keys k WHERE k.id=")
        .push_bind(id);
    Ok(query.build_query_scalar().fetch_one(connection).await?)
}

pub(in super::super) fn insert_managed_key(input: &ManagedKeyCreate) -> Statement<'_> {
    sqlx::query("INSERT INTO managed_keys(id,name,purpose,key_algorithm,secret_encrypted,fingerprint) VALUES(?,?,?,?,?,?)").bind(input.id).bind(input.name.trim()).bind(&input.purpose).bind(&input.algorithm).bind(&input.protected).bind(input.fingerprint.as_slice())
}

pub(in super::super) fn lock_managed_key(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM managed_keys WHERE id=?").bind(id)
}

pub(in super::super) fn revoke_managed_key(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE managed_keys SET revoked_at=COALESCE(revoked_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)),secret_encrypted=NULL WHERE id=?").bind(id)
}

#[async_trait::async_trait]
impl ManagedKeyStore for Backend {
    async fn replacement_preview(
        &self,
        principal: &Principal,
        source: Uuid,
        input: KeyReplacementInput,
    ) -> Result<KeyReplacementPreview> {
        transactions::key_replacement_preview(&self.write, principal, source, input).await
    }
    async fn replacement(&self, principal: &Principal, id: Uuid) -> Result<ReplacementRow> {
        transactions::key_replacement(&self.write, principal, id).await
    }
    async fn replace(
        &self,
        principal: &Principal,
        id: Uuid,
        input: Option<ManagedKeyCreate>,
    ) -> Result<KeyReplacementResult> {
        transactions::replace_keys(&self.write, principal, id, input).await
    }

    async fn nonce_observations(&self) -> Result<Vec<crate::model::key_usage::NonceRow>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,c.reserved_id,p.reserved_id,(SELECT count(*) FROM storages s WHERE s.block_key_id=k.id),EXISTS(SELECT 1 FROM chunk_encodings e WHERE e.block_key_id=k.id AND e.state='uploading') OR EXISTS(SELECT 1 FROM packs p WHERE p.block_key_id=k.id AND p.state='preparing') OR EXISTS(SELECT 1 FROM chunks c WHERE c.block_key_id=k.id AND c.state IN ('preparing','uploading')) OR EXISTS(SELECT 1 FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.block_key_id=k.id) OR EXISTS(SELECT 1 FROM tasks t WHERE replace(lower(json_extract(t.detail,'$.target_key_id')),'-','')=lower(hex(k.id)) AND t.state IN ('queued','running','paused','failed')),o.data,o.revision FROM managed_keys k LEFT JOIN nonce_counters c ON c.block_key_id=k.id AND c.type_tag=0 LEFT JOIN nonce_counters p ON p.block_key_id=k.id AND p.type_tag=1 LEFT JOIN nonce_observations o ON o.block_key_id=k.id WHERE k.purpose='block' AND k.revoked_at IS NULL ORDER BY k.id").fetch_all(&self.read).await?)
    }
    async fn save_nonce_history(
        &self,
        key: Uuid,
        previous: Option<i64>,
        history: crate::model::key_usage::NonceHistory,
    ) -> Result<()> {
        sqlx::query("INSERT INTO nonce_observations(block_key_id,data,revision) VALUES(?,?,1) ON CONFLICT(block_key_id) DO UPDATE SET data=excluded.data,revision=nonce_observations.revision+1 WHERE nonce_observations.revision=?").bind(key).bind(sqlx::types::Json(history)).bind(previous.unwrap_or(0)).execute(&self.write).await?;
        Ok(())
    }

    async fn reserve_nonce(&self, key: Uuid, type_tag: u8, units: u32) -> Result<(i64, i64)> {
        anyhow::ensure!(
            self.database_info.synchronous.as_deref() != Some("off"),
            s3s::s3_error!(
                InvalidRequest,
                "encryption requires SQLite FULL or NORMAL synchronization; save the setting and restart"
            )
        );
        transactions::reserve_nonce(
            &self.write,
            key,
            type_tag,
            units,
            self.database_info.synchronous.as_deref() == Some("normal"),
        )
        .await
    }

    async fn resolve_block(&self, name: &str) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT id,name,key_algorithm AS algorithm FROM managed_keys WHERE name=? AND purpose='block' AND revoked_at IS NULL").bind(name).fetch_optional(&self.read).await?)
    }
    async fn active_block(&self, id: Uuid) -> Result<Option<(String, String)>> {
        Ok(sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=? AND purpose='block' AND revoked_at IS NULL").bind(id).fetch_optional(&self.read).await?)
    }
    async fn for_storage(&self, storage: Uuid) -> Result<Option<ManagedKeyRecord>> {
        Ok(sqlx::query_as("SELECT k.id,k.name,k.key_algorithm AS algorithm FROM storages s JOIN managed_keys k ON k.id=s.block_key_id WHERE s.id=? AND k.purpose='block' AND k.revoked_at IS NULL").bind(storage).fetch_optional(&self.read).await?)
    }
    async fn material(&self, name: &str) -> Result<Option<ManagedKeyMaterial>> {
        Ok(sqlx::query_as("SELECT id,name,purpose,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE name=? AND secret_encrypted IS NOT NULL").bind(name).fetch_optional(&self.read).await?)
    }
    async fn list(&self, principal: &Principal) -> Result<Vec<ManagedKeyView>> {
        transactions::list_managed_keys(&self.read, principal).await
    }
    async fn create(
        &self,
        principal: &Principal,
        input: ManagedKeyCreate,
    ) -> Result<ManagedKeyView> {
        transactions::create_managed_key(&self.write, principal, input).await
    }
    async fn revoke(&self, principal: &Principal, id: Uuid) -> Result<()> {
        transactions::revoke_managed_key(&self.write, principal, id).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            nonce_capacity: None,
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            purpose: row.try_get("purpose")?,
            algorithm: row.try_get("algorithm")?,
            in_use: row.try_get("in_use")?,
            fingerprint: row.try_get("fingerprint")?,
            created_at: date(row, "created_at")?,
            revoked_at: optional_date(row, "revoked_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyRecord {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            algorithm: row.try_get("algorithm")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for ManagedKeyMaterial {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
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

pub(in super::super) fn task_value(row: SqliteRow, detailed: bool) -> Result<Value> {
    let mut value = serde_json::json!({
        "id": row.try_get::<Uuid,_>("id")?,
        "task_type": row.try_get::<String,_>("task_type")?,
        "bucket_id": row.try_get::<Option<Uuid>,_>("bucket_id")?,
        "state": row.try_get::<String,_>("state")?,
        "cursor": row.try_get::<Option<String>,_>("progress_cursor")?,
        "processed": row.try_get::<i64,_>("processed")?,
        "detail": row.try_get::<Value,_>("detail")?,
        "error": row.try_get::<Option<String>,_>("error")?,
        "created_by_label": row.try_get::<String,_>("created_by_label")?,
        "source": row.try_get::<String,_>("source")?,
        "created_at": date(&row,"created_at")?,
        "updated_at": date(&row,"updated_at")?,
        "started_at": optional_date(&row,"started_at")?,
    });
    if detailed {
        value["bucket_name"] = row.try_get::<Option<String>, _>("bucket_name")?.into();
        value["blocked"] = row.try_get::<Option<String>, _>("blocked")?.into();
    }
    Ok(value)
}
