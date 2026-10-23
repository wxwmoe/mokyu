//! PostgreSQL transaction boundaries and lock ordering.
use crate::model::Id;
use crate::model::multipart::{RemoteCommit, RemoteOutput, RemotePart, RemoteUpload};
use crate::model::task::Execution;

use super::{Backend, normalize_error, queries};
use crate::app::{Bucket, Extent};
use crate::authorization::{Action, DEFAULT_PROJECT, Permit, Principal};
use crate::content::codec::Chunk;
use crate::model::account::{
    Preferences, RefreshedSession, SessionLogin, SessionRefresh, VerifiedAccount, csrf_token,
    derived_token,
};
use crate::model::administration::BucketGrant;
use crate::model::administration::{
    Membership, Project, ProjectInput, User, UserCreate, UserPatch,
};
use crate::model::bucket::{
    BucketImpact, BucketUpdate, CreateBucket, Domain, TransferInput, TransferProject, Website,
    fingerprint,
};
use crate::model::credential::{
    self, Credential, CredentialInput, CredentialMaterial, CredentialSettings, RotateCredential,
    Token, TokenInput, TokenMaterial,
};
use crate::model::key::{ManagedKeyCreate, ManagedKeyView};
use crate::model::multipart::{
    MultipartCreate, PartStitch, SeedConsumption, Transfer, TransferFilter,
};
use crate::model::settings::{ControlSettingsPatch, RuntimeSettingsRow};
use crate::model::{
    ChunkCreate, ObjectChange, PublishStore, QuotaStore, StreamCreate, StreamStore, StreamTail,
    check_write_conditions,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

fn denied() -> anyhow::Error {
    s3s::s3_error!(AccessDenied).into()
}

async fn create_refresh(
    tx: &mut Transaction<'_, Postgres>,
    session: Uuid,
    family: Uuid,
    hash: &[u8],
) -> Result<Uuid> {
    Ok(queries::insert_refresh(session, family, hash)
        .fetch_one(&mut **tx)
        .await?)
}

async fn replace_session_refresh(
    tx: &mut Transaction<'_, Postgres>,
    session: Uuid,
    hash: &[u8],
) -> Result<()> {
    queries::revoke_refreshes(session)
        .execute(&mut **tx)
        .await?;
    create_refresh(tx, session, Uuid::new_v4(), hash).await?;
    Ok(())
}

pub(super) async fn login_session(pool: &PgPool, input: SessionLogin<'_>) -> Result<()> {
    let (user, verified_hash, revision) = input.verified;
    let mut tx = pool.begin().await?;
    // Password reset and disable update this same row before revoking sessions.
    let current = queries::lock_login_user(*user)
        .fetch_optional(&mut *tx)
        .await?;
    if !current.is_some_and(|(hash, enabled, current_revision)| {
        enabled && hash == *verified_hash && current_revision == *revision
    }) {
        return Err(denied());
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("login-before-session").await;
    let session = queries::insert_session(&input).fetch_one(&mut *tx).await?;
    create_refresh(&mut tx, session, Uuid::new_v4(), input.refresh_hash).await?;
    security_audit(&mut tx, *user, "session.login", true, input.request_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn failed_login(pool: &PgPool, user: Uuid, request: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    security_audit(&mut tx, user, "session.login", false, request).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn refresh_session(
    pool: &PgPool,
    input: SessionRefresh<'_>,
) -> Result<RefreshedSession> {
    let hash = blake3::hash(input.supplied.as_bytes());
    let mut tx = pool.begin().await?;
    let user = queries::refresh_user(hash.as_bytes())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    // Match password reset and session revocation lock order.
    let revision = queries::lock_refresh_user(user)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    let mut credential = queries::lock_refresh(hash.as_bytes())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    let now = Utc::now();
    let mut secret = input.supplied.to_owned();
    let mut issue_refresh = false;
    if let Some(revoked) = credential.revoked_at {
        let successor = queries::lock_refresh_successor(credential.replaced_by)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(next) = successor.filter(|_| revoked + chrono::Duration::seconds(30) > now) {
            // Concurrent rotation responses return identical credentials without storing secrets.
            secret = derived_token(input.key, "mokyu-refresh-v1", input.supplied);
            credential = next;
            issue_refresh = true;
        } else {
            let active =
                queries::active_refresh_family(credential.session_id, credential.family_id)
                    .fetch_one(&mut *tx)
                    .await?;
            if active {
                queries::delete_refresh_session(credential.session_id)
                    .execute(&mut *tx)
                    .await?;
                security_audit(
                    &mut tx,
                    user,
                    "session.refresh_reuse",
                    false,
                    input.request_id,
                )
                .await?;
                tx.commit().await?;
            }
            return Err(denied());
        }
    }
    if credential.expires_at <= now {
        return Err(denied());
    }
    let expires = queries::lock_refresh_session(credential.session_id, user, revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(denied)?;
    if credential.expires_at <= now + chrono::Duration::days(7) {
        secret = derived_token(input.key, "mokyu-refresh-v1", &secret);
        queries::revoke_refresh(credential.id)
            .execute(&mut *tx)
            .await?;
        let new_id = create_refresh(
            &mut tx,
            credential.session_id,
            credential.family_id,
            blake3::hash(secret.as_bytes()).as_bytes(),
        )
        .await?;
        queries::replace_refresh(credential.id, new_id)
            .execute(&mut *tx)
            .await?;
        credential.id = new_id;
        issue_refresh = true;
    }
    let access = derived_token(input.key, "mokyu-session-v1", &secret);
    let csrf = csrf_token(&access);
    let issue_access = issue_refresh
        || input.access != Some(access.as_str())
        || expires <= now + chrono::Duration::seconds((input.seconds / 4).max(1) as i64);
    let expires = if issue_access {
        now + chrono::Duration::seconds(input.seconds as i64)
    } else {
        expires
    };
    queries::refresh_session(
        credential.session_id,
        blake3::hash(access.as_bytes()).as_bytes(),
        blake3::hash(csrf.as_bytes()).as_bytes(),
        expires,
    )
    .execute(&mut *tx)
    .await?;
    queries::touch_refresh(credential.id)
        .execute(&mut *tx)
        .await?;
    security_audit(&mut tx, user, "session.refresh", true, input.request_id).await?;
    tx.commit().await?;
    Ok(RefreshedSession {
        user,
        session: credential.session_id,
        access,
        csrf,
        refresh: secret,
        expires,
        refreshed_at: now,
        issue_access,
    })
}

async fn lock_account_session(
    tx: &mut Transaction<'_, Postgres>,
    user: Uuid,
    current: &[u8],
) -> Result<(String, i64)> {
    let (hash, revision, enabled) = queries::lock_session_user(user)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(denied)?;
    if !enabled
        || !queries::active_session(user, current, revision)
            .fetch_one(&mut **tx)
            .await?
    {
        return Err(denied());
    }
    Ok((hash, revision))
}

async fn lock_verified_account(
    tx: &mut Transaction<'_, Postgres>,
    verified: &VerifiedAccount,
    current: &[u8],
) -> Result<()> {
    let (hash, revision) = lock_account_session(tx, verified.0, current).await?;
    if hash != verified.1 || revision != verified.2 {
        return Err(denied());
    }
    Ok(())
}

pub(super) async fn logout_session(pool: &PgPool, user: Uuid, current: &[u8]) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    queries::delete_access_session(current)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "session.logout",
        &user.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn account_preferences(
    pool: &PgPool,
    user: Uuid,
    current: &[u8],
    input: Preferences,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    queries::account_preferences(user, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "account.preferences",
        &user.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn account_password(
    pool: &PgPool,
    verified: &VerifiedAccount,
    current: &[u8],
    password: &str,
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_verified_account(&mut tx, verified, current).await?;
    queries::change_password(verified.0, password)
        .execute(&mut *tx)
        .await?;
    queries::delete_other_sessions(verified.0, current)
        .execute(&mut *tx)
        .await?;
    queries::session_revision(current, verified.2 + 1)
        .execute(&mut *tx)
        .await?;
    let session = queries::session_id(current).fetch_one(&mut *tx).await?;
    replace_session_refresh(&mut tx, session, refresh).await?;
    checkpoint_event(
        &mut tx,
        "account.password",
        &verified.0.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn reauthenticate_account(
    pool: &PgPool,
    verified: &VerifiedAccount,
    current: &[u8],
    refresh: &[u8],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_verified_account(&mut tx, verified, current).await?;
    queries::reauthenticate_session(current)
        .execute(&mut *tx)
        .await?;
    let session = queries::session_id(current).fetch_one(&mut *tx).await?;
    replace_session_refresh(&mut tx, session, refresh).await?;
    checkpoint_event(
        &mut tx,
        "session.reauthenticate",
        &verified.0.to_string(),
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn revoke_account_sessions(
    pool: &PgPool,
    user: Uuid,
    current: &[u8],
    session: Option<Uuid>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_account_session(&mut tx, user, current).await?;
    if let Some(id) = session {
        queries::delete_account_session(id, user)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "session.revoke",
            &id.to_string(),
            serde_json::json!({}),
        )
        .await?;
    } else {
        queries::delete_other_sessions(user, current)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "session.revoke_others",
            &user.to_string(),
            serde_json::json!({}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn account_setup_token(pool: &PgPool, hash: &[u8]) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_setup_metadata().execute(&mut *tx).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    queries::save_setup_token(hash).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn initialize_admin(
    pool: &PgPool,
    username: &str,
    password: &str,
    require_change: bool,
) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    ensure!(
        !queries::has_accounts().fetch_one(&mut *tx).await?,
        "an administrator already exists"
    );
    let id = queries::insert_initial_admin(username, password, require_change)
        .fetch_one(&mut *tx)
        .await?;
    security_audit(&mut tx, id, "account.init", true, "cli-init").await?;
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn setup_account(
    pool: &PgPool,
    username: &str,
    password: &str,
    supplied: &[u8],
    request: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    if !queries::setup_allowed(supplied).fetch_one(&mut *tx).await? {
        return Err(denied());
    }
    let user = queries::insert_setup_admin(username, password)
        .fetch_one(&mut *tx)
        .await?;
    security_audit(&mut tx, user, "account.setup", true, request).await?;
    queries::delete_setup_tokens().execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

struct ChunkClaim(
    Option<(
        Transaction<'static, Postgres>,
        tokio::sync::OwnedSemaphorePermit,
    )>,
);

impl Drop for ChunkClaim {
    fn drop(&mut self) {
        if let Some((transaction, permit)) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            // A cancelled advisory wait still occupies its connection until rollback completes.
            runtime.spawn(async move {
                if let Err(error) = transaction.rollback().await {
                    tracing::warn!(%error, "chunk claim release failed");
                }
                drop(permit);
            });
        }
    }
}

pub(super) async fn claim_chunk(
    pool: &PgPool,
    storage: Uuid,
    hash: &[u8; 32],
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<Box<dyn Send>> {
    let mut claim = ChunkClaim(Some((pool.begin().await?, permit)));
    queries::claim_chunk_hash(storage, hash)
        .execute(&mut *claim.0.as_mut().unwrap().0)
        .await?;
    Ok(Box::new(claim))
}

pub(super) async fn allocate_chunk(pool: &PgPool, input: ChunkCreate) -> Result<(Chunk, bool)> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, input.stream).await?;
    queries::fail_chunk_allocations(&input)
        .execute(&mut *tx)
        .await?;
    let old = queries::reusable_chunk(&input)
        .fetch_optional(&mut *tx)
        .await?;
    let reused = old.is_some();
    let chunk = match old {
        Some(chunk) => Some(chunk),
        None => {
            queries::insert_chunk(&input)
                .fetch_optional(&mut *tx)
                .await?
        }
    };
    let chunk = chunk.ok_or_else(|| {
        s3s::s3_error!(OperationAborted, "chunk allocation is already in progress")
    })?;
    if reused {
        require_sources(&mut tx, None, &[chunk.id]).await?;
    }
    tx.commit().await?;
    Ok((chunk, reused))
}

pub(super) async fn reference_chunk(
    pool: &PgPool,
    stream: Id,
    offset: i64,
    chunk_id: Id,
    raw_size: i32,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, stream).await?;
    let state: String = queries::lock_chunk(chunk_id).fetch_one(&mut *tx).await?;
    if state != "ready" {
        return Err(s3s::s3_error!(OperationAborted, "chunk is being reclaimed").into());
    }
    require_sources(&mut tx, None, &[chunk_id]).await?;
    queries::insert_chunk_extent(stream, offset, raw_size, chunk_id)
        .execute(&mut *tx)
        .await?;
    queries::reference_chunk(chunk_id).execute(&mut *tx).await?;
    queries::pin_chunk(chunk_id, stream)
        .execute(&mut *tx)
        .await?;
    queries::attach_pending_upload(chunk_id, stream, offset)
        .execute(&mut *tx)
        .await?;
    queries::extend_writing_stream(stream, offset + raw_size as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn complete_chunk_write(
    pool: &PgPool,
    stream: Id,
    offset: i64,
    chunk: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, stream).await?;
    ensure!(
        queries::allocated_source(stream, chunk)
            .build_query_scalar::<Id>()
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let changed = queries::complete_chunk(chunk.id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    queries::complete_encoding(chunk.encoding_id)
        .execute(&mut *tx)
        .await?;
    if changed != 1 {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::insert_chunk_extent(stream, offset, chunk.raw_size, chunk.id)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(stream, offset + chunk.raw_size as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn copy_chunk_references(
    pool: &PgPool,
    destination: Id,
    source: Id,
    offset: i64,
    end: i64,
    chunks: &[Id],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, destination).await?;
    let locked: Vec<Id> = queries::lock_ready_chunks(chunks)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(locked.len() == chunks.len(), "copy source is not ready");
    require_sources(&mut tx, None, chunks).await?;
    queries::copy_extents(destination, source, offset, end)
        .execute(&mut *tx)
        .await?;
    queries::reference_chunks(chunks).execute(&mut *tx).await?;
    queries::pin_chunks(chunks, destination)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(destination, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn reserve_quota(
    pool: &PgPool,
    stream: Id,
    size: i64,
    padding: bool,
    upload: Option<(uuid::Uuid, i32)>,
) -> Result<i64> {
    Ok(queries::reserve_quota(
        stream,
        size,
        padding,
        upload.map(|u| u.0),
        upload.map(|u| u.1),
    )
    .fetch_one(pool)
    .await?)
}

pub(super) async fn complete_quota(
    pool: &PgPool,
    upload: uuid::Uuid,
    stream: Id,
    size: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let bucket: uuid::Uuid = queries::lock_completing_upload(upload)
        .fetch_one(&mut *tx)
        .await?;
    queries::lock_stream(stream).execute(&mut *tx).await?;
    queries::lock_quota(bucket).execute(&mut *tx).await?;
    // Reuse the accepted parts' reservation, including omitted parts until publication succeeds.
    let changed = queries::complete_quota(upload, stream, size)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    anyhow::ensure!(changed == 1, "multipart quota reservation is missing");
    tx.commit().await?;
    Ok(())
}

#[async_trait::async_trait]
impl QuotaStore for Backend {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
        read_quota(&self.pool, principal, scope, id).await
    }
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota> {
        save_quota(&self.pool, principal, scope, id, input).await
    }

    async fn reserve(
        &self,
        stream: Id,
        size: i64,
        padding: bool,
        upload: Option<(uuid::Uuid, i32)>,
    ) -> Result<i64> {
        reserve_quota(&self.pool, stream, size, padding, upload)
            .await
            .map_err(normalize_error)
    }

    async fn complete(&self, upload: uuid::Uuid, stream: Id, size: i64) -> Result<()> {
        complete_quota(&self.pool, upload, stream, size)
            .await
            .map_err(normalize_error)
    }
}

#[async_trait::async_trait]
impl PublishStore for Backend {
    async fn publish(
        &self,
        id: Id,
        if_match: Option<s3s::dto::ETagCondition>,
        if_none: Option<s3s::dto::ETagCondition>,
        completion: Option<(uuid::Uuid, String, Value, Uuid)>,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let authority: sqlx::types::Json<crate::authorization::Permit> =
            queries::write_authorization(id).fetch_one(&mut *tx).await?;
        authority.lock(&mut tx).await?;
        let (bucket, object_key, state, size, payload_type): (
            uuid::Uuid,
            String,
            String,
            i64,
            String,
        ) = queries::lock_writing_stream(id).fetch_one(&mut *tx).await?;
        let bucket_state: String = queries::lock_bucket_state(bucket)
            .fetch_one(&mut *tx)
            .await?;
        if bucket_state != "active" {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
        let (old, epoch): (Option<Id>, Id) = queries::lock_object_version(bucket, &object_key)
            .fetch_one(&mut *tx)
            .await?;
        if epoch != id || state != "writing" {
            return Err(s3s::s3_error!(OperationAborted, "write was superseded").into());
        }
        if let Some((upload, _, _, _)) = &completion {
            let expected: Option<(Option<Id>, i64)> = queries::web_upload_expectation(upload)
                .fetch_optional(&mut *tx)
                .await?;
            if let Some((version, expected_size)) = expected {
                if old != version {
                    return Err(s3s::s3_error!(
                        PreconditionFailed,
                        "object changed since upload started"
                    )
                    .into());
                }
                if size != expected_size {
                    return Err(s3s::s3_error!(IncompleteBody).into());
                }
            }
        }
        let previous: Option<String> = if let Some(old) = old {
            Some(queries::stream_etag(old).fetch_one(&mut *tx).await?)
        } else {
            None
        };
        check_write_conditions(previous.as_deref(), if_match.as_ref(), if_none.as_ref())?;
        if payload_type == "raw" {
            validate_raw(&mut tx, id, size).await?;
        } else {
            let invalid: bool = queries::incomplete_extents(id).fetch_one(&mut *tx).await?;
            anyhow::ensure!(!invalid, "cannot publish an incomplete generation");
            let (end, contiguous): (i64, bool) =
                queries::extent_continuity(id).fetch_one(&mut *tx).await?;
            anyhow::ensure!(
                end == size && contiguous,
                "cannot publish a mapping with gaps or overlaps"
            );
            require_sources(&mut tx, Some(id), &[]).await?;
        }
        if !apply_publish(&mut tx, id, bucket, &object_key, old, completion.as_ref()).await? {
            return Ok(false);
        }
        if let Some((upload, _, _, _)) = &completion {
            checkpoint_event(
                &mut tx,
                "upload.complete",
                &upload.to_string(),
                serde_json::json!({"bucket_id":bucket,"key":object_key,"size":size}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}

pub(super) async fn recover_stale(pool: &PgPool) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let stale: Vec<uuid::Uuid> = queries::lock_stale_tasks().fetch_all(&mut *tx).await?;
    if stale.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }
    queries::abandon_task_streams(&stale)
        .execute(&mut *tx)
        .await?;
    let result = queries::requeue_stale_tasks(&stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result.rows_affected())
}

pub(super) async fn claim_task(
    pool: &PgPool,
    input: crate::model::TaskClaim<'_>,
    node: Option<(
        &crate::cluster::NodeState,
        &str,
        &crate::content::codec::Key,
    )>,
) -> Result<Option<uuid::Uuid>> {
    let mut tx = pool.begin().await?;
    if let Some((state, role, key)) = node {
        validate_node(&mut tx, state, role, key).await?;
    }
    let id = queries::claim_task(
        input.class,
        input.active,
        node.map(|(state, _, _)| state.node_id),
        input.token,
        input.draining,
    )
    .fetch_optional(&mut *tx)
    .await?;
    // The new execution owns cleanup before any fresh physical outputs are allocated.
    if input.class != 0
        && let Some(id) = id
    {
        release_task_inputs(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(crate) async fn validate_node(
    db: &mut PgConnection,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
) -> Result<()> {
    let (registered_role, enabled, encrypted) = queries::lock_node(state.node_id)
        .fetch_optional(db)
        .await?
        .context("registered node no longer exists")?;
    ensure!(
        enabled && registered_role == role,
        "registered node is disabled or belongs to another role"
    );
    let expected = crate::content::codec::unprotect(
        encrypted.as_deref().context("node has no credential")?,
        key,
        crate::cluster::credential_binding(state.node_id).as_bytes(),
    )?;
    ensure!(
        aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            state.credential.as_bytes()
        )
        .is_ok(),
        "invalid node credential"
    );
    Ok(())
}

pub(super) async fn try_acquire_storage_permit(
    pool: &PgPool,
    storage_id: uuid::Uuid,
    direction: &str,
    limit: i32,
    holder: uuid::Uuid,
) -> Result<Option<uuid::Uuid>> {
    let token = uuid::Uuid::new_v4();
    let mut tx = pool.begin().await?;
    queries::lock_storage_permits(format!("mokyu:store:{storage_id}:{direction}"))
        .execute(&mut *tx)
        .await?;
    queries::expire_storage_permits(storage_id, direction)
        .execute(&mut *tx)
        .await?;
    let active: i64 = queries::storage_permit_count(storage_id, direction)
        .fetch_one(&mut *tx)
        .await?;
    if active >= i64::from(limit) {
        tx.rollback().await?;
        return Ok(None);
    }
    queries::insert_storage_permit(token, storage_id, direction, holder)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(token))
}

pub(super) async fn renew_storage_permit(pool: &PgPool, token: uuid::Uuid) -> Result<bool> {
    Ok(queries::renew_storage_permit(token)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(super) async fn release_storage_permit(pool: &PgPool, token: uuid::Uuid) -> Result<()> {
    queries::release_storage_permit(token).execute(pool).await?;
    Ok(())
}

pub(crate) async fn insert_task(
    db: &mut PgConnection,
    id: uuid::Uuid,
    task_type: &str,
    bucket: Option<uuid::Uuid>,
    detail: Value,
    audit_id: Option<Id>,
) -> Result<()> {
    queries::insert_task(id, task_type, bucket, detail, audit_id)
        .execute(&mut *db)
        .await?;
    queries::notify_task(id.to_string())
        .execute(&mut *db)
        .await?;
    Ok(())
}

pub(super) async fn finish_stream(
    pool: &PgPool,
    id: Id,
    size: i64,
    etag: &str,
    checksums: Value,
) -> Result<bool> {
    Ok(queries::finish_stream(id, size, etag, checksums)
        .execute(pool)
        .await?
        .rows_affected()
        == 1)
}

pub(super) async fn apply_publish(
    tx: &mut Transaction<'_, Postgres>,
    id: Id,
    bucket: uuid::Uuid,
    object_key: &str,
    old: Option<Id>,
    completion: Option<&(uuid::Uuid, String, Value, Uuid)>,
) -> Result<bool> {
    queries::publish_object(bucket, object_key, id)
        .execute(&mut **tx)
        .await?;
    queries::publish_stream(id).execute(&mut **tx).await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut **tx).await?;
    }
    if let Some((upload, hash, result, token)) = completion {
        let changed = queries::complete_upload(upload, hash, result, id, *token)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if changed != 1 {
            return Ok(false);
        }
        queries::retire_upload_parts(&upload)
            .execute(&mut **tx)
            .await?;
        queries::delete_upload_parts(upload)
            .execute(&mut **tx)
            .await?;
    }
    Ok(true)
}

pub(super) async fn apply_object_change(
    tx: &mut Transaction<'_, Postgres>,
    bucket: uuid::Uuid,
    object_key: &str,
    old: Option<Id>,
    public: Option<bool>,
) -> Result<()> {
    if let Some(public) = public {
        let id = old.context("object stream is required for ACL changes")?;
        queries::set_stream_public(id, public)
            .execute(&mut **tx)
            .await?;
    } else {
        queries::remove_object(bucket, object_key, Id::default())
            .execute(&mut **tx)
            .await?;
        if let Some(old) = old {
            queries::retire_stream(old).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

pub(super) async fn change_object(pool: &PgPool, input: ObjectChange) -> Result<bool> {
    let mut tx = pool.begin().await?;
    Permit::for_action(
        input.principal,
        input.bucket,
        if input.public.is_some() {
            crate::authorization::Action::Acl
        } else {
            crate::authorization::Action::Delete
        },
    )
    .lock(&mut tx)
    .await?;
    let state: Option<String> = queries::lock_bucket_state(input.bucket)
        .fetch_optional(&mut *tx)
        .await?;
    let state = state.ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted, "bucket is being purged").into());
    }
    let old: Option<Id> = queries::lock_object_stream(input.bucket, &input.key)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
    if input.expected.is_some() && input.expected != old {
        return Err(s3s::s3_error!(
            PreconditionFailed,
            "object changed; refresh before retrying"
        )
        .into());
    }
    if let Some(public) = input.public {
        let id = old.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        apply_object_change(&mut tx, input.bucket, &input.key, old, Some(public)).await?;
        complete_media_operation(&mut tx, Some(id)).await?;
        tx.commit().await?;
        return Ok(false);
    }
    apply_object_change(&mut tx, input.bucket, &input.key, old, None).await?;
    complete_media_operation(&mut tx, None).await?;
    tx.commit().await?;
    Ok(true)
}

#[async_trait::async_trait]
impl StreamStore for Backend {
    async fn snapshot(
        &self,
        route: &crate::model::ReadRoute,
        access: Option<&str>,
    ) -> Result<Option<crate::model::ReadSnapshot>> {
        queries::read_snapshot(&self.pool, route, access).await
    }
    async fn cached(&self, bucket: Uuid, key: &str) -> Result<Option<crate::model::CachedStream>> {
        queries::cached_stream(&self.pool, bucket, key).await
    }

    async fn current(&self, bucket: Uuid, key: &str) -> Result<crate::app::StoredStream> {
        queries::current_stream(bucket, key, false)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey).into())
    }

    async fn create(&self, input: StreamCreate) -> Result<Id> {
        create_stream(&self.pool, input, None).await
    }

    async fn append_tail(&self, tail: StreamTail) -> Result<()> {
        queries::insert_fragment(&tail).execute(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        #[cfg(feature = "fault-injection")]
        crate::faults::point("fragment-persisted").await;
        queries::seal_fragment(tail.fragment.into())
            .execute(&mut *tx)
            .await?;
        queries::insert_fragment_extent(tail.stream, tail.offset, tail.size, tail.fragment.into())
            .execute(&mut *tx)
            .await?;
        queries::resize_stream(tail.stream, tail.offset + i64::from(tail.size))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn finish(&self, stream: Id, size: i64, etag: &str, checksums: Value) -> Result<bool> {
        finish_stream(&self.pool, stream, size, etag, checksums).await
    }

    async fn learn_compression(
        &self,
        observation: crate::model::CompressionObservation,
        node: Option<Uuid>,
    ) -> Result<bool> {
        observation.validate()?;
        Ok(queries::learn_compression(observation, node)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }
}

pub(super) async fn current_stream(
    pool: &PgPool,
    bucket: Uuid,
    key: &str,
    lease_seconds: i64,
) -> Result<(crate::app::StoredStream, Uuid)> {
    let mut tx = pool.begin().await?;
    let stream: crate::app::StoredStream = queries::current_stream(bucket, key, true)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let lease = create_read_lease(&mut tx, stream.id, lease_seconds).await?;
    tx.commit().await?;
    Ok((stream, lease))
}

pub(super) async fn create_stream(
    pool: &PgPool,
    mut input: StreamCreate,
    ttl_seconds: Option<i64>,
) -> Result<Id> {
    let mut tx = pool.begin().await?;
    if let Some((task, token)) = input.owner.as_ref() {
        let owned: Option<Uuid> = queries::lock_owned_task(task, token)
            .fetch_optional(&mut *tx)
            .await?;
        anyhow::ensure!(owned.is_some(), "storage migration lease lost");
    }
    input
        .authority
        .add(input.bucket, crate::authorization::Action::Write);
    if input.public {
        input
            .authority
            .add(input.bucket, crate::authorization::Action::Acl);
    }
    input.authority.lock(&mut tx).await?;
    let (state, paused): (String, bool) = queries::lock_bucket_uploads(input.bucket)
        .fetch_one(&mut *tx)
        .await?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if paused && input.stream_type == "object" {
        let finishing: bool =
            queries::completing_upload(input.completion, input.bucket, &input.key)
                .fetch_one(&mut *tx)
                .await?;
        if !finishing {
            return Err(
                s3s::s3_error!(OperationAborted, "new uploads are paused for this bucket").into(),
            );
        }
    }
    let id = queries::insert_stream(&input).fetch_one(&mut *tx).await?;
    if let Some(ttl) = ttl_seconds {
        create_write_lease(&mut tx, id, ttl).await?;
    }
    if input.claim_object {
        queries::claim_object(input.bucket, &input.key, id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn create_read_lease(
    tx: &mut Transaction<'_, Postgres>,
    stream: Id,
    ttl_seconds: i64,
) -> Result<uuid::Uuid> {
    Ok(queries::create_read_lease(stream, ttl_seconds)
        .fetch_one(&mut **tx)
        .await?)
}

pub(super) async fn create_write_lease(
    tx: &mut Transaction<'_, Postgres>,
    stream: Id,
    ttl_seconds: i64,
) -> Result<()> {
    queries::create_write_lease(stream, ttl_seconds)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) async fn lock_identity(
    db: &mut PgConnection,
    principal: &crate::authorization::Principal,
) -> Result<bool> {
    queries::validate_identity(principal, db, true).await
}

pub(crate) async fn lock_permit(
    tx: &mut PgConnection,
    permit: &crate::authorization::Permit,
) -> Result<()> {
    let admin = queries::validate_identity(&permit.principal, tx, true).await?;
    for (bucket, required) in &permit.resources {
        let exists: Option<Uuid> = queries::permission_bucket(*bucket)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let available = queries::bucket_actions(&permit.principal, tx, *bucket, admin).await?;
        if required.iter().any(|a| !available.contains(a)) {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
    }
    Ok(())
}

pub(crate) async fn checkpoint_audit(
    db: &mut PgConnection,
    id: Id,
    action: &str,
    target: &str,
    project: Option<Uuid>,
    bucket: Option<Uuid>,
    detail: Value,
) -> Result<()> {
    let changed = queries::checkpoint_audit(id, action, target, project, bucket, detail)
        .execute(db)
        .await?
        .rows_affected();
    ensure!(changed == 1, "audit intent is missing");
    Ok(())
}

pub(crate) async fn partial_audit(db: &mut PgConnection, id: Id) -> Result<()> {
    queries::partial_audit(id).execute(db).await?;
    Ok(())
}

async fn security_audit(
    db: &mut PgConnection,
    user: Uuid,
    action: &str,
    success: bool,
    request: &str,
) -> Result<()> {
    // Serialize rejected password events to keep the existing per-minute bound.
    queries::lock_audit_user(user).execute(&mut *db).await?;
    queries::security_audit(user, action, success, request)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn admin_transaction<'a>(
    pool: &'a PgPool,
    principal: &Principal,
) -> Result<Transaction<'a, Postgres>> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    principal.lock_admin(&mut tx).await?;
    Ok(tx)
}

async fn enable_member_mode(tx: &mut Transaction<'_, Postgres>, role: &str) -> Result<()> {
    if role == "member" {
        queries::enable_project_mode().execute(&mut **tx).await?;
    }
    Ok(())
}

async fn protect_admin(
    tx: &mut Transaction<'_, Postgres>,
    current: &User,
    role: &str,
    enabled: bool,
) -> Result<()> {
    if current.role == "admin" && current.enabled && (role != "admin" || !enabled) {
        let count: i64 = queries::admin_count().fetch_one(&mut **tx).await?;
        if count <= 1 {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    Ok(())
}

pub(super) async fn create_user(
    pool: &PgPool,
    principal: &Principal,
    input: UserCreate,
) -> Result<User> {
    let mut tx = admin_transaction(pool, principal).await?;
    if input.role == "member" {
        let admin: bool = queries::has_admin().fetch_one(&mut *tx).await?;
        if !admin {
            return Err(crate::model::Error::LastAdministrator.into());
        }
    }
    enable_member_mode(&mut tx, &input.role).await?;
    let row: Option<User> = queries::insert_user(&input)
        .fetch_optional(&mut *tx)
        .await?;
    let row = row.ok_or_else(|| s3s::s3_error!(OperationAborted))?;
    queries::delete_setup_tokens().execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.create",
        &row.id.to_string(),
        serde_json::json!({"username":row.username,"role":row.role,"enabled":row.enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn update_user(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    input: UserPatch,
) -> Result<User> {
    let mut tx = admin_transaction(pool, principal).await?;
    // Mode changes lock metadata before the target user so project settings cannot race.
    queries::lock_project_mode().execute(&mut *tx).await?;
    let current: User = queries::lock_user(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let role = input.role.as_deref().unwrap_or(&current.role);
    let enabled = input.enabled.unwrap_or(current.enabled);
    protect_admin(&mut tx, &current, role, enabled).await?;
    enable_member_mode(&mut tx, role).await?;
    if role == "admin" && current.role != "admin" {
        queries::delete_user_memberships(id)
            .execute(&mut *tx)
            .await?;
    }
    let row = queries::update_user(id, role, enabled)
        .fetch_one(&mut *tx)
        .await?;
    queries::delete_user_sessions(id).execute(&mut *tx).await?;
    checkpoint_event(&mut tx,"user.update",&id.to_string(),serde_json::json!({"username":current.username,"old_role":current.role,"role":role,"enabled":enabled})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn delete_user(pool: &PgPool, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let current: User = queries::lock_user(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    protect_admin(&mut tx, &current, "member", false).await?;
    queries::delete_user(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.delete",
        &id.to_string(),
        serde_json::json!({"username":current.username}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn reset_password(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    hash: &str,
    require_change: bool,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let changed = queries::reset_user_password(id, hash, require_change)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    queries::delete_user_sessions(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "user.reset_password",
        &id.to_string(),
        serde_json::json!({"must_change_password":require_change}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn save_member(
    pool: &PgPool,
    principal: &Principal,
    project: Uuid,
    user: Uuid,
    input: Option<Membership>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    enable_member_mode(&mut tx, "member").await?;
    let target: Option<String> = queries::lock_member(user).fetch_optional(&mut *tx).await?;
    if target.as_deref() != Some("member") {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let exists: bool = queries::project_exists(project).fetch_one(&mut *tx).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let audit_detail = serde_json::json!({"project_id":project,"user_id":user,"membership":input});
    if let Some(input) = input {
        let mut buckets: Vec<Uuid> = input.grants.iter().map(|g| g.bucket_id).collect();
        buckets.sort();
        let valid: Vec<Uuid> = queries::lock_project_buckets(&buckets, project)
            .fetch_all(&mut *tx)
            .await?;
        if valid != buckets {
            return Err(s3s::s3_error!(InvalidArgument).into());
        }
        queries::save_membership(user, project, &input.role, &input.scope)
            .execute(&mut *tx)
            .await?;
        queries::delete_member_grants(user, project)
            .execute(&mut *tx)
            .await?;
        for grant in input.grants {
            queries::insert_member_grant(user, project, &grant)
                .execute(&mut *tx)
                .await?;
        }
    } else {
        queries::delete_membership(user, project)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "project.membership",
        &user.to_string(),
        audit_detail,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn save_project(
    pool: &PgPool,
    principal: &Principal,
    id: Option<Uuid>,
    input: ProjectInput,
) -> Result<Project> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_project_mode().execute(&mut *tx).await?;
    let duplicate: bool = queries::duplicate_project(input.name.trim(), id)
        .fetch_one(&mut *tx)
        .await?;
    if duplicate {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let row: Project = if let Some(id) = id {
        queries::update_project(id, &input)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchKey))?
    } else {
        queries::enable_project_mode().execute(&mut *tx).await?;
        queries::insert_project(&input).fetch_one(&mut *tx).await?
    };
    checkpoint_event(&mut tx,if id.is_some(){"project.update"}else{"project.create"},&row.id.to_string(),serde_json::json!({"project_id":row.id,"name":row.name,"allow_bucket_create":row.allow_bucket_create})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn delete_project(pool: &PgPool, principal: &Principal, id: Uuid) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let exists: Option<Uuid> = queries::lock_project(id).fetch_optional(&mut *tx).await?;
    if exists.is_none() {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let dependent: bool = queries::project_dependents(id).fetch_one(&mut *tx).await?;
    if dependent {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::delete_project(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "project.delete",
        &id.to_string(),
        serde_json::json!({"project_id":id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn set_project_mode(
    pool: &PgPool,
    principal: &Principal,
    enabled: bool,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_project_mode().execute(&mut *tx).await?;
    if !enabled {
        let dependent: bool = queries::project_mode_dependents()
            .fetch_one(&mut *tx)
            .await?;
        if dependent {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    queries::set_project_mode(enabled).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "project.mode",
        "",
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get_credential(
    connection: &mut PgConnection,
    access: &str,
) -> Result<Credential> {
    Ok(queries::credential_record(access)
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn replace_credential_grants(
    tx: &mut Transaction<'_, Postgres>,
    access: &str,
    project: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    credential::validate_grants(&grants)?;
    let mut ids: Vec<_> = grants.iter().map(|g| g.bucket_id).collect();
    ids.sort();
    let found: Vec<Uuid> = queries::lock_project_buckets(&ids, project)
        .fetch_all(&mut **tx)
        .await?;
    if found != ids {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    queries::delete_credential_grants(access)
        .execute(&mut **tx)
        .await?;
    for grant in grants {
        queries::insert_credential_grant(access, &grant)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn insert_credential(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    input: CredentialInput,
    material: CredentialMaterial,
) -> Result<Credential> {
    credential::label(&input.label)?;
    credential::validate_grants(&input.grants)?;
    let expires = credential::expiration(input.expires_in.as_deref())?;
    let project: Option<Uuid> = queries::credential_project(input.project_id)
        .fetch_optional(&mut **tx)
        .await?;
    if project.is_none() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let access = &material.access;
    queries::insert_credential(&material, &input, expires, principal.user_id())
        .execute(&mut **tx)
        .await?;
    replace_credential_grants(tx, access, input.project_id, input.grants).await?;
    let credential = get_credential(tx, access).await?;
    Ok(credential)
}

pub(super) async fn create_credential(
    pool: &PgPool,
    principal: &Principal,
    input: CredentialInput,
    material: CredentialMaterial,
) -> Result<Credential> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = insert_credential(&mut tx, principal, input, material).await?;
    checkpoint_event(&mut tx,"credential.create",&row.access_key,serde_json::json!({"project_id":row.project_id,"label":row.label,"grants":row.grants,"expires_at":row.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn update_credential(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
    input: CredentialSettings,
) -> Result<Credential> {
    credential::label(&input.label)?;
    let expires = credential::expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = admin_transaction(pool, principal).await?;
    let changed = queries::update_credential(access, &input, expires)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    if let Some(grants) = input.grants {
        let row = get_credential(&mut tx, access).await?;
        replace_credential_grants(&mut tx, access, row.project_id, grants).await?;
    }
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(&mut tx,"credential.update",access,serde_json::json!({"project_id":row.project_id,"label":row.label,"enabled":row.enabled,"grants":row.grants,"expires_at":row.expires_at})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn set_grants(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
    grants: Vec<BucketGrant>,
) -> Result<Credential> {
    let mut tx = admin_transaction(pool, principal).await?;
    let project = queries::lock_credential_project(access)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    replace_credential_grants(&mut tx, access, project, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn revoke_credential(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::delete_credential(access).execute(&mut *tx).await?;
    checkpoint_event(&mut tx, "credential.delete", access, serde_json::json!({})).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn enable_credential(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
    enabled: bool,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let n = queries::enable_credential(access, enabled)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n == 0 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    checkpoint_event(
        &mut tx,
        "credential.enabled",
        access,
        serde_json::json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn change_grant(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
    bucket: Uuid,
    actions: Vec<crate::authorization::Action>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_credential(access).execute(&mut *tx).await?;
    let current = get_credential(&mut tx, access).await?;
    let mut grants = current.grants;
    grants.retain(|g| g.bucket_id != bucket);
    if !actions.is_empty() {
        grants.push(BucketGrant {
            bucket_id: bucket,
            actions,
        });
    }
    replace_credential_grants(&mut tx, access, current.project_id, grants).await?;
    let row = get_credential(&mut tx, access).await?;
    checkpoint_event(
        &mut tx,
        "credential.permissions",
        access,
        serde_json::json!({"project_id":row.project_id,"grants":row.grants}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn rotate_credential(
    pool: &PgPool,
    principal: &Principal,
    access: &str,
    input: RotateCredential,
    material: CredentialMaterial,
) -> Result<(Credential, chrono::DateTime<Utc>)> {
    let seconds =
        crate::config::seconds(&input.overlap).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if seconds > 30 * 86400 {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = admin_transaction(pool, principal).await?;
    queries::lock_credential(access).execute(&mut *tx).await?;
    let current = get_credential(&mut tx, access).await?;
    if !current.enabled {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let old_expires_at = current
        .expires_at
        .unwrap_or(Utc::now() + chrono::Duration::days(30))
        .min(Utc::now() + chrono::Duration::seconds(seconds as i64));
    let replacement = insert_credential(
        &mut tx,
        principal,
        CredentialInput {
            project_id: current.project_id,
            label: current.label,
            expires_in: input.expires_in,
            grants: current.grants,
        },
        material,
    )
    .await?;
    queries::expire_credential(access, old_expires_at)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"credential.rotate",access,serde_json::json!({"project_id":replacement.project_id,"replacement_key":replacement.access_key,"old_expires_at":old_expires_at})).await?;
    tx.commit().await?;
    Ok((replacement, old_expires_at))
}

pub(super) async fn get_token(db: &mut PgConnection, id: Uuid) -> Result<Token> {
    Ok(queries::token_record(id)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

async fn token_owner<'a>(
    pool: &'a PgPool,
    principal: &Principal,
    user: Uuid,
    other: bool,
) -> Result<Transaction<'a, Postgres>> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let control = admin
        && matches!(
            principal,
            Principal::System | Principal::Local | Principal::Node { .. }
        );
    if !control && principal.user_id() != Some(user) && !(other && admin) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    queries::lock_token_owner(user).execute(&mut *tx).await?;
    Ok(tx)
}

async fn token_scope(
    tx: &mut Transaction<'_, Postgres>,
    user: Uuid,
    input: &TokenInput,
) -> Result<i64> {
    credential::label(&input.label)?;
    credential::validate_grants(&input.grants)?;
    let (role, revision): (String, i64) = queries::token_owner_revision(user)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(AccessDenied))?;
    if input.system && (role != "admin" || !input.grants.is_empty()) {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    let mut grants: Vec<_> = input.grants.iter().collect();
    grants.sort_by_key(|g| g.bucket_id);
    for grant in grants {
        let exists: Option<Uuid> = queries::permission_bucket(grant.bucket_id)
            .fetch_optional(&mut **tx)
            .await?;
        if exists.is_none() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        if role != "admin" {
            let actions: Option<Vec<String>> = queries::token_owner_actions(user, grant.bucket_id)
                .fetch_optional(&mut **tx)
                .await?;
            if grant.actions.iter().any(|a| {
                !actions
                    .as_ref()
                    .is_some_and(|list| list.iter().any(|v| v == a.name()))
            }) {
                return Err(s3s::s3_error!(AccessDenied).into());
            }
        }
    }
    Ok(revision)
}

async fn replace_token_grants(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    grants: Vec<BucketGrant>,
) -> Result<()> {
    queries::delete_token_grants(id).execute(&mut **tx).await?;
    for grant in grants {
        queries::insert_token_grant(id, &grant)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

pub(super) async fn create_token(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    input: TokenInput,
    material: TokenMaterial,
) -> Result<Token> {
    if input.keep_expiry {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let expires = credential::expiration(input.expires_in.as_deref())?;
    let mut tx = token_owner(pool, principal, user, false).await?;
    let revision = token_scope(&mut tx, user, &input).await?;
    let id = queries::insert_token(user, &input, &material, revision, expires)
        .fetch_one(&mut *tx)
        .await?;
    replace_token_grants(&mut tx, id, input.grants).await?;
    let token = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.create",&id.to_string(),serde_json::json!({"user_id":user,"label":token.label,"system":token.system,"grants":token.grants,"expires_at":token.expires_at})).await?;
    tx.commit().await?;
    Ok(token)
}

pub(super) async fn update_token(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    id: Uuid,
    input: TokenInput,
) -> Result<Token> {
    let expires = credential::expiration(input.expires_in.as_deref())?;
    if input.keep_expiry && expires.is_some() {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = token_owner(pool, principal, user, false).await?;
    queries::lock_token(id).execute(&mut *tx).await?;
    let current = get_token(&mut tx, id).await?;
    if current.user_id != user || current.revoked_at.is_some() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    token_scope(&mut tx, user, &input).await?;
    queries::update_token(id, &input, expires)
        .execute(&mut *tx)
        .await?;
    replace_token_grants(&mut tx, id, input.grants).await?;
    let result = get_token(&mut tx, id).await?;
    checkpoint_event(&mut tx,"token.update",&id.to_string(),serde_json::json!({"user_id":user,"label":result.label,"system":result.system,"grants":result.grants,"expires_at":result.expires_at})).await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn revoke_token(pool: &PgPool, principal: &Principal, id: Uuid) -> Result<()> {
    let user: Uuid = queries::token_user(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut tx = token_owner(pool, principal, user, true).await?;
    queries::revoke_token(id).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "token.revoke",
        &id.to_string(),
        serde_json::json!({"user_id":user}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn lock_bucket_settings(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    bucket: Uuid,
) -> Result<()> {
    principal.lock_identity(tx).await?;
    queries::lock_bucket_id(bucket).execute(&mut **tx).await?;
    Permit::for_action(principal.clone(), bucket, Action::Settings)
        .lock(tx)
        .await
}

pub(super) async fn create_bucket(
    pool: &PgPool,
    principal: &Principal,
    input: CreateBucket,
) -> Result<Bucket> {
    if !s3s::path::check_bucket_name(&input.name) {
        return Err(s3s::s3_error!(InvalidBucketName).into());
    }
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let project = input.project_id.unwrap_or(DEFAULT_PROJECT);
    let allowed: Option<bool> = queries::bucket_creation_policy(project)
        .fetch_optional(&mut *tx)
        .await?;
    let allowed = allowed.ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let mut selected = false;
    if !admin {
        if !allowed || !matches!(principal, Principal::User { .. }) {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let member: Option<(String, String)> =
            queries::bucket_creator_membership(project, principal.user_id())
                .fetch_optional(&mut *tx)
                .await?;
        let Some((role, scope)) = member else {
            return Err(s3s::s3_error!(AccessDenied).into());
        };
        if role != "maintainer" {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        selected = scope == "selected";
    }
    let available: Vec<Uuid> = queries::bucket_available_storages()
        .fetch_all(&mut *tx)
        .await?;
    let storage_id = match input.storage_id {
        Some(id) if available.contains(&id) => id,
        Some(_) => return Err(anyhow::anyhow!("storage is unavailable").into()),
        None if available.len() == 1 => available[0],
        None if available.is_empty() => {
            return Err(anyhow::anyhow!("no storage configured").into());
        }
        None => return Err(anyhow::anyhow!("storage must be selected").into()),
    };
    let row: Option<Bucket> = queries::insert_bucket(&input.name, project, storage_id)
        .fetch_optional(&mut *tx)
        .await?;
    let row = row.ok_or(crate::model::Error::BucketAlreadyExists)?;
    if selected {
        queries::grant_bucket_creator(principal.user_id(), project, row.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.create",
        &row.name,
        json!({"bucket_id":row.id,"project_id":project}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}

pub(crate) async fn lock_bucket_dependents(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<()> {
    // Identity rows precede the bucket, matching admitted requests and grant mutations.
    queries::lock_bucket_members(bucket)
        .execute(&mut **tx)
        .await?;
    queries::lock_bucket_credentials(bucket)
        .execute(&mut **tx)
        .await?;
    queries::lock_bucket_tokens(bucket)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) async fn bucket_impact(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
) -> Result<BucketImpact> {
    Ok(queries::bucket_impact(bucket)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
}

pub(super) async fn delete_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    name: &str,
    confirmation: Option<&str>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    let row: Bucket = queries::lock_bucket(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if row.name != name {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if let Some(expected) = confirmation
        && fingerprint(&bucket_impact(&mut tx, bucket).await?, None)? != expected
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    let busy: bool = queries::bucket_has_streams(bucket)
        .fetch_one(&mut *tx)
        .await?;
    if busy || row.state != "active" {
        return Err(crate::model::Error::BucketNotEmpty.into());
    }
    queries::delete_bucket_objects(bucket)
        .execute(&mut *tx)
        .await?;
    queries::delete_bucket(bucket).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "bucket.delete",
        name,
        json!({"bucket_id":bucket,"project_id":row.project_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn domain_change(
    pool: &PgPool,
    principal: &Principal,
    value: &str,
    bucket: Option<Uuid>,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    let old: Option<Uuid> = queries::bucket_domain_owner(value)
        .fetch_optional(&mut *tx)
        .await?;
    if bucket.is_some() && old.is_some() && old != bucket {
        return Err(crate::model::Error::DomainInUse.into());
    }
    if let Some(id) = bucket.or(old) {
        let state: Option<String> = queries::bucket_domain_state(id)
            .fetch_optional(&mut *tx)
            .await?;
        if state.as_deref() != Some("active") {
            return Err(s3s::s3_error!(OperationAborted).into());
        }
    }
    if let Some(id) = bucket {
        queries::insert_bucket_domain(value, id)
            .execute(&mut *tx)
            .await?;
    } else {
        queries::delete_bucket_domain(value)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(
        &mut tx,
        "bucket.domain",
        value,
        json!({"bucket_id":bucket.or(old),"removed":bucket.is_none()}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn bucket_transfer_target(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<TransferProject> {
    Ok(queries::bucket_transfer_target(id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?)
}

pub(super) async fn save_bucket_settings(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    mut input: BucketUpdate,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    if input.domains.is_some() {
        queries::lock_accounts().execute(&mut *tx).await?;
    }
    let admin = principal.lock_identity(&mut tx).await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let row: Bucket = queries::lock_bucket(bucket).fetch_one(&mut *tx).await?;
    if row.settings_revision != input.revision {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if row.state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if !admin
        && (input.domains.is_some()
            || row.uploads_paused != input.uploads_paused
            || row.public_base_url != input.public_base_url)
    {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    if let Some(domains) = input.domains.take() {
        let used: bool = queries::bucket_domains_in_use(&domains, bucket)
            .fetch_one(&mut *tx)
            .await?;
        if used {
            return Err(crate::model::Error::DomainInUse.into());
        }
        queries::remove_bucket_domains(bucket, &domains)
            .execute(&mut *tx)
            .await?;
        queries::insert_bucket_domains(&domains, bucket)
            .execute(&mut *tx)
            .await?;
    }
    queries::save_bucket_settings(bucket, &input)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.settings",&row.name,json!({"bucket_id":bucket,"project_id":row.project_id,"uploads_paused":input.uploads_paused})).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn transfer_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    input: TransferInput,
) -> Result<Bucket> {
    let mut tx = admin_transaction(pool, principal).await?;
    let source: Uuid = queries::bucket_project_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    if source == input.target_project {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    queries::lock_bucket_transfer_users(source, input.target_project)
        .execute(&mut *tx)
        .await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    queries::lock_bucket_id(bucket).execute(&mut *tx).await?;
    let current = bucket_impact(&mut tx, bucket).await?;
    if current.name != input.confirm_name
        || fingerprint(&current, Some(input.target_project))? != input.confirmation
    {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if current.state != "active"
        || !current.uploads_paused
        || [
            &current.reserved_bytes,
            &current.inflight_bytes,
            &current.active_uploads,
            &current.writing_streams,
        ]
        .iter()
        .any(|s| s.as_str() != "0")
    {
        return Err(crate::model::Error::BucketNotDrained.into());
    }
    bucket_transfer_target(&mut tx, input.target_project).await?;
    queries::revoke_bucket_members(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_credentials(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_tokens(bucket)
        .execute(&mut *tx)
        .await?;
    queries::revoke_bucket_transfer_users(source, input.target_project)
        .execute(&mut *tx)
        .await?;
    let row: Bucket = queries::transfer_bucket(bucket, input.target_project)
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(&mut tx,"bucket.transfer",&row.name,json!({"bucket_id":bucket,"project_id":input.target_project,"previous_project":source,"revoked_member_bucket_grants":current.member_bucket_grants,"revoked_service_grants":current.service_grants,"revoked_token_bucket_grants":current.token_bucket_grants})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn bucket_settings(pool: &PgPool, bucket: Uuid) -> Result<(Bucket, Vec<String>)> {
    let mut tx = pool.begin().await?;
    queries::bucket_snapshot().execute(&mut *tx).await?;
    let row = queries::bucket_by_id(bucket)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    let domains = queries::bucket_domain_names(bucket)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((row, domains))
}

pub(super) async fn bucket_domains(
    pool: &PgPool,
    principal: &Principal,
    bucket: Option<Uuid>,
) -> Result<Vec<Domain>> {
    let tx = admin_transaction(pool, principal).await?;
    tx.commit().await?;
    Ok(queries::bucket_domains(bucket).fetch_all(pool).await?)
}

pub(super) async fn preview_bucket(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
) -> Result<BucketImpact> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    bucket_impact(&mut tx, bucket).await
}

pub(super) async fn preview_bucket_transfer(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    target: Uuid,
) -> Result<(BucketImpact, TransferProject)> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let bucket = bucket_impact(&mut tx, bucket).await?;
    let target = bucket_transfer_target(&mut tx, target).await?;
    if bucket.project_id == target.id {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    Ok((bucket, target))
}

pub(super) async fn set_bucket_cors(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    rules: Value,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let result = queries::set_bucket_cors(bucket, rules)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    checkpoint_event(
        &mut tx,
        "bucket.cors",
        &bucket.to_string(),
        serde_json::json!({"bucket_id":bucket,"cors":result}),
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn set_bucket_website(
    pool: &PgPool,
    principal: &Principal,
    bucket: Uuid,
    input: Website,
) -> Result<Website> {
    let mut tx = pool.begin().await?;
    lock_bucket_settings(&mut tx, principal, bucket).await?;
    let row = queries::set_bucket_website(bucket, &input)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?;
    checkpoint_event(&mut tx,"bucket.website",&bucket.to_string(),serde_json::json!({"bucket_id":bucket,"website_enabled":input.website_enabled,"index_document":input.index_document,"error_document":input.error_document})).await?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn runtime_settings_sections(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Vec<(String, Value, i64)>> {
    let mut tx = admin_transaction(pool, principal).await?;
    let rows = queries::runtime_settings().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}
pub(super) async fn runtime_settings_section(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::runtime_settings_row(section)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn save_runtime_settings(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
    revision: Option<i64>,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let revision = match revision {
        Some(revision) => revision,
        None => {
            queries::lock_runtime_settings(section)
                .fetch_one(&mut *tx)
                .await?
        }
    };
    let row = queries::save_runtime_settings(section, value, principal.user_id(), revision)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            s3s::s3_error!(
                OperationAborted,
                "settings revision conflict; reload before saving"
            )
        })?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn reset_runtime_settings(
    pool: &PgPool,
    principal: &Principal,
    section: &str,
    value: Value,
) -> Result<RuntimeSettingsRow> {
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::reset_runtime_settings(section, value, principal.user_id())
        .fetch_one(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "runtime.settings",
        section,
        json!({"revision":row.revision}),
    )
    .await?;
    tx.commit().await?;
    Ok(row)
}
pub(super) async fn update_web_settings(
    pool: &PgPool,
    principal: &Principal,
    input: ControlSettingsPatch,
) -> Result<(crate::config::WebSettings, i64)> {
    let mut tx = admin_transaction(pool, principal).await?;
    let current = queries::lock_web_settings().fetch_one(&mut *tx).await?;
    let mut control = current.control()?;
    if let Some(value) = input.origin {
        control.origin = value;
    }
    if let Some(value) = input.secure_cookie {
        control.secure_cookie = value;
    }
    if let Some(value) = input.session_lifetime {
        control.session_lifetime = value;
    }
    if let Some(value) = input.gravatar_base_url {
        control.gravatar_base_url = value;
    }
    control.validate()?;
    if let Some(revision) = input.revision {
        anyhow::ensure!(
            revision == current.revision,
            s3s::s3_error!(
                OperationAborted,
                "settings revision conflict; reload before saving"
            )
        );
    }
    let value = serde_json::json!({
        "origin": control.origin,
        "secure_cookie": control.secure_cookie,
        "session_lifetime": control.session_lifetime,
        "gravatar_base_url": control.gravatar_base_url,
    });
    let next_revision = current
        .revision
        .checked_add(1)
        .context("settings revision overflow")?;
    queries::save_web_settings(value, next_revision, principal.user_id(), current.revision)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "control.settings",
        "",
        serde_json::json!({
            "origin": control.origin,
            "secure_cookie": control.secure_cookie,
            "session_lifetime": control.session_lifetime,
            "gravatar_base_url": control.gravatar_base_url,
        }),
    )
    .await?;
    tx.commit().await?;
    Ok((control, next_revision))
}

pub(super) async fn list_managed_keys(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Vec<ManagedKeyView>> {
    let tx = admin_transaction(pool, principal).await?;
    tx.commit().await?;
    Ok(queries::managed_key_views()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(queries::managed_key_view)
        .collect())
}
pub(super) async fn create_managed_key(
    pool: &PgPool,
    principal: &Principal,
    input: ManagedKeyCreate,
) -> Result<ManagedKeyView> {
    let mut tx = admin_transaction(pool, principal).await?;
    let result = queries::managed_key_view(
        queries::insert_managed_key(&input)
            .fetch_one(&mut *tx)
            .await?,
    );
    if input.purpose == "block" {
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    }
    checkpoint_event(&mut tx, "managed_key.create", &input.id.to_string(), json!({"name":result.name,"purpose":result.purpose,"algorithm":result.algorithm,"fingerprint":result.fingerprint})).await?;
    tx.commit().await?;
    Ok(result)
}
pub(super) async fn revoke_managed_key(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<()> {
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    queries::lock_managed_key(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if queries::managed_key_in_use(id).fetch_one(&mut *tx).await? {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "key is still used by a storage, physical encoding, pending upload or task"
        )
        .into());
    }
    if queries::revoke_managed_key(id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    checkpoint_event(&mut tx, "managed_key.revoke", &id.to_string(), json!({})).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn checkpoint_event(
    tx: &mut Transaction<'_, Postgres>,
    action: &str,
    target: &str,
    detail: Value,
) -> Result<()> {
    let Some(id) = crate::model::audit::current() else {
        return Ok(());
    };
    let project = detail
        .get("project_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    let bucket = detail
        .get("bucket_id")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok());
    ensure!(
        serde_json::to_vec(&detail)?.len() <= 262144,
        "audit detail exceeds its bound"
    );
    checkpoint_audit(tx, id, action, target, project, bucket, detail).await
}

pub(super) async fn create_multipart(
    pool: &PgPool,
    input: MultipartCreate<'_>,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    input.authority.lock(&mut tx).await?;
    let (state, paused) = queries::multipart_bucket(input.bucket)
        .fetch_one(&mut *tx)
        .await?;
    if state != "active" {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    let expected = if let Some(web) = &input.web {
        if let Some((id, hash)) = queries::web_upload_identity(web.user, web.client_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            if hash != web.request_hash {
                return Err(
                    s3s::s3_error!(InvalidRequest, "upload identity was already used").into(),
                );
            }
            return Ok((id, true));
        }
        let expected = queries::web_upload_expected(input.bucket, input.key)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
        if expected.is_some() && !web.overwrite {
            return Err(crate::model::Error::ObjectAlreadyExists.into());
        }
        expected
    } else {
        None
    };
    if paused {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if let Some(limit) = input.max_active {
        queries::lock_upload_admission().execute(&mut *tx).await?;
        if queries::active_upload_count().fetch_one(&mut *tx).await? >= limit {
            return Err(s3s::s3_error!(SlowDown).into());
        }
    }
    let id = queries::insert_multipart(&input)
        .fetch_one(&mut *tx)
        .await?;
    if let Some(web) = input.web {
        queries::insert_web_upload(id, &web, expected)
            .execute(&mut *tx)
            .await?;
        checkpoint_event(
            &mut tx,
            "upload.create",
            &id.to_string(),
            json!({"bucket_id":input.bucket,"key":input.key,"size":web.size}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

pub(super) async fn begin_upload_part(
    pool: &PgPool,
    authority: &Permit,
    upload: Uuid,
    number: i32,
    stream: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    authority.lock(&mut tx).await?;
    if queries::lock_upload_state(upload)
        .fetch_one(&mut *tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    queries::begin_upload_part(upload, number, stream)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_upload_part(
    pool: &PgPool,
    authority: &Permit,
    bucket: Uuid,
    upload: Uuid,
    number: i32,
    stream: Id,
    remote: Option<&RemoteCommit<'_>>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    authority.lock(&mut tx).await?;
    if let Some(input) = remote {
        raw_upload_processor(&mut tx, input.node).await?;
    }
    if queries::multipart_bucket_state(bucket)
        .fetch_one(&mut *tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    if queries::lock_upload_state(upload)
        .fetch_one(&mut *tx)
        .await?
        != "active"
    {
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    let (old, epoch) = queries::lock_upload_part(upload, number)
        .fetch_one(&mut *tx)
        .await?;
    if epoch != stream {
        return Err(s3s::s3_error!(OperationAborted, "part was superseded").into());
    }
    require_writing_stream(&mut tx, stream).await?;
    let (_, _, _, size, payload) = queries::lock_writing_stream(stream)
        .fetch_one(&mut *tx)
        .await?;
    if payload == "raw" {
        validate_raw(&mut tx, stream, size).await?;
    } else {
        require_sources(&mut tx, Some(stream), &[]).await?;
    }
    if let Some(input) = remote {
        ensure!(
            queries::remote_part_queue(input)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    queries::publish_upload_part(upload, number, stream)
        .execute(&mut *tx)
        .await?;
    queries::ready_upload_stream(stream)
        .execute(&mut *tx)
        .await?;
    if let Some(old) = old {
        queries::retire_upload_stream(old).execute(&mut *tx).await?;
    }
    queries::touch_upload(upload).execute(&mut *tx).await?;
    checkpoint_event(
        &mut tx,
        "upload.part",
        &upload.to_string(),
        json!({"bucket_id":bucket,"part":number}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn consume_upload_seed(pool: &PgPool, input: SeedConsumption<'_>) -> Result<()> {
    let mut tx = pool.begin().await?;
    let active = queries::lock_upload_active(input.upload)
        .fetch_one(&mut *tx)
        .await?;
    let mut streams: Vec<_> = input.spans.iter().map(|span| span.0).collect();
    streams.push(input.stream);
    streams.sort_unstable();
    streams.dedup();
    lock_splice_sources(&mut tx, &streams, input.chunk).await?;
    let mut remaining = input.prefix;
    let mut chunk_offset = 0;
    while remaining > 0 {
        let span = input.spans.first_mut().context("missing prefix span")?;
        let n = remaining.min(span.2);
        if active {
            replace_upload_range(&mut tx, span.0, span.1, n, input.chunk, chunk_offset).await?;
        }
        span.1 += n as i64;
        span.2 -= n;
        remaining -= n;
        chunk_offset += n;
        if span.2 == 0 {
            input.spans.remove(0);
        }
    }
    if input.prefix == input.consumed {
        queries::remove_upload_extent(input.stream, input.offset)
            .execute(&mut *tx)
            .await?;
        mark_unreferenced(&mut tx, &[input.chunk]).await?;
    } else {
        queries::slice_upload_extent(
            input.stream,
            input.offset,
            input.prefix as i32,
            (input.consumed - input.prefix) as i32,
        )
        .execute(&mut *tx)
        .await?;
    }
    queries::resize_upload_stream(
        input.stream,
        input.offset + (input.consumed - input.prefix) as i64,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn stitch_upload_parts(pool: &PgPool, input: PartStitch) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_splice_sources(&mut tx, &[input.left, input.right], input.chunk).await?;
    replace_upload_range(
        &mut tx,
        input.left,
        input.offset,
        input.left_length,
        input.chunk,
        0,
    )
    .await?;
    if input.right_length > 0 {
        replace_upload_range(
            &mut tx,
            input.right,
            0,
            input.right_length,
            input.chunk,
            input.left_length,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn begin_upload_completion(
    pool: &PgPool,
    input: crate::model::multipart::Completion<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    input.authority.lock(&mut tx).await?;
    raw_upload_processor(&mut tx, input.node).await?;
    let (state, hash, live) = queries::completion_state(input.upload)
        .fetch_one(&mut *tx)
        .await?;
    if matches!(state.as_str(), "completing" | "completed") {
        ensure!(
            hash.as_deref() == Some(input.hash),
            s3s::s3_error!(InvalidRequest, "upload has another completion manifest")
        );
        if live || state == "completed" {
            return Ok(false);
        }
    } else {
        ensure!(state == "active", s3s::s3_error!(OperationAborted));
    }
    let parts: std::collections::BTreeMap<_, _> = queries::completion_parts(input.upload)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect();
    ensure!(
        !input.parts.is_empty()
            && input
                .parts
                .iter()
                .all(|(number, stream)| parts.get(number) == Some(stream)),
        s3s::s3_error!(InvalidPart, "selected part generation changed")
    );
    queries::begin_upload_completion(&input)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn abort_multipart(
    pool: &PgPool,
    authority: &Permit,
    upload: Uuid,
    expired: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    authority.lock(&mut tx).await?;
    if expired
        && queries::lock_upload_state(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            != Some("active")
    {
        return Ok(false);
    }
    if queries::abort_multipart(upload, expired)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        if expired {
            return Ok(false);
        }
        return Err(s3s::s3_error!(NoSuchUpload).into());
    }
    queries::retire_upload_parts(&upload)
        .execute(&mut *tx)
        .await?;
    queries::delete_upload_parts(&upload)
        .execute(&mut *tx)
        .await?;
    if authority.principal.user_id().is_some() && crate::model::audit::active() {
        let bucket = queries::upload_bucket(upload).fetch_one(&mut *tx).await?;
        checkpoint_event(
            &mut tx,
            "upload.abort",
            &upload.to_string(),
            json!({"bucket_id":bucket}),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn upload_transfers(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    filter: &TransferFilter,
    after: Option<(chrono::DateTime<Utc>, Uuid)>,
    id: Option<Uuid>,
    limit: i64,
) -> Result<(Vec<Transfer>, bool)> {
    let mut tx = pool.begin().await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let mut query =
        queries::upload_transfers(admin, user, principal.token_id(), filter, after, id, limit);
    let mut rows: Vec<Transfer> = query.build_query_as().fetch_all(&mut *tx).await?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let ready: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.remote_state == "stored")
        .map(|r| r.id)
        .collect();
    if !ready.is_empty() {
        queries::durable_uploads(&ready).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok((rows, more))
}

async fn mark_unreferenced(tx: &mut Transaction<'_, Postgres>, chunks: &[Id]) -> Result<()> {
    if !chunks.is_empty() {
        queries::mark_unreferenced_chunks(chunks)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn replace_upload_range(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    stream: Id,
    offset: i64,
    length: usize,
    chunk: Id,
    source: usize,
) -> Result<()> {
    if length == 0 {
        return Ok(());
    }
    let end = offset + length as i64;
    let rows: Vec<Extent> = queries::lock_upload_extents(stream, offset, end)
        .fetch_all(&mut **tx)
        .await?;
    let mut covered = offset;
    let mut previous = Vec::new();
    for row in rows {
        if let Some(id) = row.chunk_id {
            previous.push(id);
        }
        ensure!(row.offset_bytes <= covered, "extent replacement gap");
        let row_end = row.offset_bytes + row.length as i64;
        covered = covered.max(row_end.min(end));
        queries::remove_upload_extent(stream, row.offset_bytes)
            .execute(&mut **tx)
            .await?;
        if row.offset_bytes < offset {
            insert_upload_extent(
                tx,
                &row,
                row.offset_bytes,
                (offset - row.offset_bytes) as i32,
                row.source_offset,
            )
            .await?;
        }
        if row_end > end {
            insert_upload_extent(
                tx,
                &row,
                end,
                (row_end - end) as i32,
                row.source_offset + (end - row.offset_bytes) as i32,
            )
            .await?;
        }
    }
    ensure!(covered == end, "extent replacement incomplete");
    queries::insert_upload_chunk_extent(stream, offset, length as i32, chunk, source as i32)
        .execute(&mut **tx)
        .await?;
    // Retained slices and references from other streams still protect the old chunk.
    mark_unreferenced(tx, &previous).await?;
    Ok(())
}

async fn insert_upload_extent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    e: &Extent,
    offset: i64,
    length: i32,
    source: i32,
) -> Result<()> {
    queries::insert_upload_extent(e, offset, length, source)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

use crate::model::cache::{CacheBatch, CacheRelease, CacheStage};

pub(super) async fn cache_stage(pool: &PgPool, input: CacheStage<'_>) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, input.node).await?;
    require_writing_stream(&mut tx, input.stream).await?;
    let (state, source) = queries::lock_cache_source(input.chunk.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        if input.new {
            matches!(state.as_str(), "preparing" | "uploading")
        } else {
            state == "ready"
        },
        "upload source changed"
    );
    if !input.new && source.is_none() {
        return Ok(false);
    }
    if !input.new {
        require_sources(&mut tx, None, &[input.chunk.id]).await?;
    }
    queries::insert_pending_upload(&input, source)
        .execute(&mut *tx)
        .await?;
    queries::insert_upload_pin(input.chunk.id, source, input.stream)
        .execute(&mut *tx)
        .await?;
    if input.buffered {
        ensure!(
            input.new && input.node.is_none(),
            "buffered chunks require single mode"
        );
        queries::buffer_chunk(input.chunk, input.boot_id)
            .execute(&mut *tx)
            .await?;
    }
    queries::ready_cached_chunk(input.chunk.id)
        .execute(&mut *tx)
        .await?;
    queries::insert_cached_extent(&input)
        .execute(&mut *tx)
        .await?;
    queries::resize_cached_stream(input.stream, input.offset + i64::from(input.chunk.raw_size))
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-cache-committed").await;
    Ok(true)
}

pub(super) async fn cache_seal(pool: &PgPool, stream: Id, eof: bool) -> Result<bool> {
    if eof {
        queries::seal_stream_uploads(stream).execute(pool).await?;
        return Ok(true);
    }
    let rows = queries::unsealed_uploads(stream).fetch_all(pool).await?;
    let mut ids = Vec::new();
    let mut total = 0u64;
    let mut end = None;
    let mut sealed = false;
    for (id, offset, size, maximum) in rows {
        let maximum = crate::config::bytes(&maximum)?;
        if !ids.is_empty() && (end != Some(offset) || total + size as u64 > maximum) {
            sealed = true;
            break;
        }
        ids.push(id);
        total += size as u64;
        end = Some(offset + i64::from(size));
        if total == maximum {
            sealed = true;
            break;
        }
    }
    if sealed {
        queries::seal_upload_chunks(&ids).execute(pool).await?;
    }
    Ok(sealed)
}

pub(super) async fn cache_flush(pool: &PgPool, node: Option<Uuid>) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    let upper = queries::flush_time().fetch_one(&mut *tx).await?;
    let nodes = if node.is_some() {
        Vec::new()
    } else {
        queries::cache_nodes().fetch_all(&mut *tx).await?
    };
    insert_task(
        &mut tx,
        id,
        "cache_flush",
        None,
        json!({"upper_time":upper,"coordinator":!nodes.is_empty(),"node_id":node}),
        crate::model::audit::current(),
    )
    .await?;
    if let Some(node) = node {
        queries::scope_upload_task(id, node, None)
            .execute(&mut *tx)
            .await?;
    }
    for node in nodes {
        let child = Uuid::new_v4();
        insert_task(
            &mut tx,
            child,
            "cache_flush",
            None,
            json!({"upper_time":upper,"node_id":node}),
            crate::model::audit::current(),
        )
        .await?;
        queries::scope_upload_task(child, node, Some(id))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn cache_enqueue(pool: &PgPool, urgent: &[Uuid]) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if let Some(id) = queries::retryable_upload().fetch_optional(&mut *tx).await? {
        queries::queue_upload_retry(id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some((first, stream, source, owner)) = queries::due_upload(urgent)
        .build_query_as::<(Id, Option<Id>, Option<Id>, Option<Uuid>)>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(false);
    };
    let (maximum, _, _) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let maximum = crate::config::bytes(&maximum)?;
    let id = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    insert_task(
        &mut tx,
        id,
        "upload",
        None,
        json!({"first":first,"stream_id":stream,"source_pack_id":source}),
        crate::model::audit::current(),
    )
    .await?;
    if let Some(node) = owner {
        queries::scope_upload_task(id, node, None)
            .execute(&mut *tx)
            .await?;
    }
    let candidates = queries::pending_upload_segment(first, stream, source)
        .fetch_all(&mut *tx)
        .await?;
    let mut ids = Vec::new();
    let mut bytes = 0u64;
    let mut end = None;
    for (chunk, offset, size) in candidates {
        if !ids.is_empty() && (end != Some(offset) || bytes + size as u64 > maximum) {
            break;
        }
        ids.push(chunk);
        bytes += size as u64;
        end = Some(offset + i64::from(size));
    }
    if !ids.contains(&first) {
        ids = vec![first];
    }
    queries::claim_pending_uploads(&ids, id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn cache_batch(
    pool: &PgPool,
    task: Execution,
    detail: &Value,
    flush: bool,
) -> Result<CacheBatch> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let first = if flush {
        queries::flush_candidate(detail, task.id)
            .fetch_optional(&mut *tx)
            .await?
    } else {
        Id::from_json(&detail["first"])?
    };
    let Some(first) = first else {
        return Ok(
            if flush && queries::flush_pending(detail).fetch_one(&mut *tx).await? {
                CacheBatch::Wait
            } else {
                CacheBatch::Finished
            },
        );
    };
    let durable = queries::upload_is_durable(first)
        .fetch_one(&mut *tx)
        .await?;
    let unused = queries::upload_is_unused(first).fetch_one(&mut *tx).await?;
    if durable || unused {
        return Ok(CacheBatch::Release(first));
    }
    let (maximum, timeout, encoding) = queries::cache_policy(first).fetch_one(&mut *tx).await?;
    let encoding: Option<crate::model::cache::EncodingPolicy> =
        encoding.map(serde_json::from_value).transpose()?;
    if let Some(policy) = &encoding {
        policy.validate()?;
    }
    let maximum = crate::config::bytes(&maximum)?;
    let timeout = crate::config::seconds(&timeout)?;
    let Some((stream, offset, source, expired)) = queries::pending_upload(first, timeout)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(CacheBatch::Finished);
    };
    if let Some(pack) = source.filter(|_| !flush) {
        let ids = queries::pending_pack_uploads(pack, task.id)
            .fetch_all(&mut *tx)
            .await?;
        record_source_inputs(&mut tx, task.id, &ids).await?;
        queries::claim_pending_uploads(&ids, task.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(CacheBatch::Rewrite { stream, pack, ids });
    }
    let all = queries::pending_upload_mappings(first, stream, offset, task.id, flush)
        .fetch_all(&mut *tx)
        .await?;
    let mut rows: Vec<crate::model::Mapping> = Vec::new();
    let mut raw = 0u64;
    for row in all {
        if let Some(last) = rows.last()
            && (last.offset_bytes + i64::from(last.chunk.raw_size) != row.offset_bytes
                || raw + row.chunk.raw_size as u64 > maximum
                || !queries::same_references(
                    last.chunk.id,
                    row.chunk.id,
                    row.offset_bytes - last.offset_bytes,
                )
                .fetch_one(&mut *tx)
                .await?)
        {
            break;
        }
        raw += row.chunk.raw_size as u64;
        rows.push(row);
        if flush || source.is_some() {
            break;
        }
    }
    if rows.is_empty() {
        return Ok(CacheBatch::Finished);
    }
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    pin_inputs(&mut tx, task.id, &ids).await?;
    let current = queries::lock_cache_inputs(&ids, task.id, flush)
        .fetch_all(&mut *tx)
        .await?;
    let mut expected: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r.chunk.id,
                r.chunk.encoding_id,
                r.pack_id,
                stream,
                r.offset_bytes,
                if r.chunk.id == first { source } else { None },
            )
        })
        .collect();
    expected.sort_unstable_by_key(|r| r.0);
    ensure!(
        current == expected,
        "upload inputs changed; retry maintenance"
    );
    queries::claim_pending_uploads(&ids, task.id)
        .execute(&mut *tx)
        .await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("upload-prepare-before-commit").await;
    tx.commit().await?;
    Ok(CacheBatch::Encode {
        stream,
        expired,
        rows,
        encoding,
    })
}

pub(super) async fn cache_finish(pool: &PgPool, task: Execution) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::unclaim_uploads(task.id).execute(&mut *tx).await?;
    queries::complete_upload_task(task.id)
        .execute(&mut *tx)
        .await?;
    release_task_inputs(&mut tx, task.id).await?;
    tx.commit().await?;
    Ok(())
}

async fn release_task_inputs(tx: &mut Transaction<'_, Postgres>, task: Uuid) -> Result<()> {
    queries::retire_task_packs(task).execute(&mut **tx).await?;
    queries::retire_task_encodings(task)
        .execute(&mut **tx)
        .await?;
    queries::release_task_inputs(task)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn cache_release(
    pool: &PgPool,
    ids: &[Id],
    existing_pack: bool,
) -> Result<CacheRelease> {
    let mut tx = pool.begin().await?;
    let keys = queries::lock_pending_chunks(ids)
        .fetch_all(&mut *tx)
        .await?;
    let staged = queries::pending_staging_objects(ids)
        .fetch_all(&mut *tx)
        .await?;
    let mut released = Vec::new();
    let mut released_ids = std::collections::HashSet::new();
    for (id, object_key) in keys {
        if queries::can_release_pending(id, existing_pack)
            .fetch_one(&mut *tx)
            .await?
        {
            queries::schedule_uploaded_pack(id)
                .execute(&mut *tx)
                .await?;
            queries::delete_pending_upload(id).execute(&mut *tx).await?;
            released_ids.insert(id);
            if let Some(key) = object_key {
                released.push(key);
            }
        }
    }
    tx.commit().await?;
    // Only a released mapping permits deletion of its shared staging object.
    Ok(CacheRelease {
        keys: released,
        staged: staged
            .into_iter()
            .filter(|(id, _, _)| released_ids.contains(id))
            .map(|(_, storage, key)| (storage, key))
            .collect(),
    })
}

pub(super) async fn cache_pins(
    pool: &PgPool,
    names: &[String],
    storage: Option<Uuid>,
) -> Result<Vec<String>> {
    let mut tx = pool.begin().await?;
    queries::lock_upload_pins(names, storage)
        .execute(&mut *tx)
        .await?;
    let owned = queries::owned_upload_pins(names, storage)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(owned)
}

pub(super) async fn claim_staging(
    pool: &PgPool,
    storage: Uuid,
    key: &str,
    bytes: u64,
    limit: u64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::lock_staging(format!("mokyu:staging:{storage}"))
        .execute(&mut *tx)
        .await?;
    if let Some(existing) = queries::staged_size(storage, key)
        .fetch_optional(&mut *tx)
        .await?
    {
        ensure!(
            existing == i64::try_from(bytes)?,
            "staging object size changed"
        );
        tx.commit().await?;
        return Ok(());
    }
    let used = queries::staged_usage(storage).fetch_one(&mut *tx).await?;
    ensure!(
        used >= 0 && (used as u64).saturating_add(bytes) <= limit,
        "shared staging quota exhausted"
    );
    queries::insert_staged_object(storage, key, i64::try_from(bytes)?)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

use crate::config;
use crate::model::packing::PackOutput;
pub(super) async fn pin_pack_inputs(
    pool: &PgPool,
    task: Execution,
    rows: &[Mapping],
) -> Result<()> {
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    pin_inputs(&mut tx, task.id, &ids).await?;
    tx.commit().await?;
    Ok(())
}

async fn pin_inputs(db: &mut PgConnection, task: Uuid, ids: &[Id]) -> Result<()> {
    let ready: Vec<Id> = queries::pack_ready_inputs(ids).fetch_all(&mut *db).await?;
    ensure!(ready == ids, "pack inputs changed; retry maintenance");
    record_source_inputs(db, task, ids).await?;
    queries::insert_pack_inputs(task, ids).execute(db).await?;
    Ok(())
}

pub(super) async fn start_pack(
    pool: &PgPool,
    single: bool,
    policy: &str,
    automatic: bool,
    node: Option<Uuid>,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:pack:{policy}"))
        .execute(&mut *tx)
        .await?;
    let existing: Option<Uuid> = queries::active_pack_task(policy)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(id) = existing {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let detail = json!({"policy":policy,"upper_time":chrono::Utc::now(),"automatic":automatic});
    let storages: Vec<(Uuid, String, i64)> = queries::pack_storages(Some(policy))
        .fetch_all(&mut *tx)
        .await?;
    let mut due = Vec::new();
    for row in storages {
        let profile = queries::storage_profile(row.0).fetch_one(&mut *tx).await?;
        if automatic
            && !queries::pack_policy_due(row.0, policy, profile.maintenance_interval(policy)?)
                .fetch_one(&mut *tx)
                .await?
        {
            continue;
        }
        queries::pack_policy_scheduled(row.0, policy)
            .execute(&mut *tx)
            .await?;
        due.push(row);
    }
    let storages = due;
    if automatic && storages.is_empty() {
        tx.commit().await?;
        return Ok(serde_json::json!({"idle":true}));
    }
    if single {
        let id = enqueue_storage_task(&mut tx, "pack", detail, &storages).await?;
        tx.commit().await?;
        return Ok(json!({"task_id":id}));
    }
    let mut parent_detail = detail.clone();
    parent_detail["role"] = json!("parent");
    let id = enqueue_task(&mut tx, "pack", None, parent_detail, false).await?;
    for (storage_id, storage_type, revision) in &storages {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["storage_id"] = json!(storage_id);
        child_detail["profile_revision"] = json!(revision);
        let child = enqueue_task(&mut tx, "pack", None, child_detail, true).await?;
        let (scope, node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, node)
            .execute(&mut *tx)
            .await?;
    }
    queries::pack_parent_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok(json!({"task_id":id}))
}
pub(super) async fn start_unpack(
    pool: &PgPool,
    single: bool,
    pack: Option<Id>,
    all: bool,
    node: Option<Uuid>,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:unpack:{all}:{pack:?}"))
        .execute(&mut *tx)
        .await?;
    if let Some(id) = queries::active_unpack_task(all.to_string(), pack.map(|v| v.to_string()))
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let mut detail = json!({"pack_id":pack,"all":all});
    let storages: Vec<(Uuid, String, i64)> = if let Some(pack_id) = pack {
        queries::unpack_storage(pack_id).fetch_all(&mut *tx).await?
    } else {
        queries::pack_storages(None).fetch_all(&mut *tx).await?
    };
    if single {
        detail["upper_time"] = json!(chrono::Utc::now());
        let id = enqueue_storage_task(&mut tx, "unpack", detail, &storages).await?;
        tx.commit().await?;
        return Ok(json!({"task_id":id}));
    }
    let mut parent_detail = detail.clone();
    parent_detail["role"] = json!("parent");
    let id = enqueue_task(&mut tx, "unpack", None, parent_detail, false).await?;
    for (storage_id, storage_type, revision) in &storages {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["storage_id"] = json!(storage_id);
        child_detail["profile_revision"] = json!(revision);
        let child = enqueue_task(&mut tx, "unpack", None, child_detail, true).await?;
        let (scope, node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, node)
            .execute(&mut *tx)
            .await?;
    }
    queries::pack_parent_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok(json!({"task_id":id}))
}
async fn enqueue_storage_task(
    tx: &mut Transaction<'_, Postgres>,
    task_type: &str,
    mut detail: Value,
    storages: &[(Uuid, String, i64)],
) -> Result<Uuid> {
    detail["execution_mode"] = json!("single");
    detail["storages"] = json!(
        storages
            .iter()
            .map(|(id, _, revision)| { json!({"storage_id":id,"profile_revision":revision}) })
            .collect::<Vec<_>>()
    );
    detail["storage_index"] = json!(0);
    let history = detail["phase"] == "history";
    if !history && let Some((id, _, revision)) = storages.first() {
        detail["storage_id"] = json!(id);
        detail["profile_revision"] = json!(revision);
    }
    let id = enqueue_task(tx, task_type, None, detail, false).await?;
    if storages.is_empty() && !history {
        queries::complete_task(id).execute(&mut **tx).await?;
    }
    Ok(id)
}

fn advance_storage_scope(detail: &mut Value) -> Result<bool> {
    let index = if detail["phase"] == "history" {
        detail["phase"] = json!("storage");
        0
    } else {
        detail["storage_index"]
            .as_u64()
            .context("missing task storage index")?
            + 1
    };
    let next = detail["storages"]
        .as_array()
        .context("missing task storages")?
        .get(index as usize)
        .cloned();
    detail["storage_index"] = json!(index);
    if let Some(next) = &next {
        detail["storage_id"] = next["storage_id"].clone();
        detail["profile_revision"] = next["profile_revision"].clone();
    }
    Ok(next.is_none())
}

pub(super) async fn advance_pack_storage(
    pool: &PgPool,
    task: Execution,
    processed: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .context("task execution expired")?;
    let done = advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, processed, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    if !done {
        crate::faults::point("pack-storage-advanced").await;
    }
    Ok(())
}

pub(super) async fn insert_pack_members(pool: &PgPool, pack: Id, rows: &[Mapping]) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut offset = 0i64;
    for (ordinal, row) in rows.iter().enumerate() {
        queries::insert_pack_member(pack, ordinal as i32, row.chunk.id, offset)
            .execute(&mut *tx)
            .await?;
        offset += i64::from(row.chunk.raw_size);
    }
    tx.commit().await?;
    Ok(())
}
pub(super) async fn commit_pack_outputs(
    pool: &PgPool,
    task: Execution,
    rows: &[Mapping],
    outputs: &[PackOutput],
    split: bool,
    settings: &crate::content::pack::Config,
    flush_seconds: f64,
) -> Result<()> {
    let mut ids: Vec<Id> = rows.iter().map(|r| r.chunk.id).collect();
    ids.sort_unstable();
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    let current: Vec<(Id, Option<Id>)> =
        queries::lock_pack_layout(&ids).fetch_all(&mut *tx).await?;
    ensure!(
        current.len() == rows.len()
            && rows.iter().all(|r| current
                .iter()
                .any(|(id, p)| *id == r.chunk.id && *p == r.pack_id)),
        "pack layout changed; retry maintenance"
    );
    check_source_inputs(&mut tx, task.id, &ids).await?;
    let before: i64 = queries::pack_layout_bytes(&ids).fetch_one(&mut *tx).await?;
    let added: i64 = queries::pack_added_bytes(task.id)
        .fetch_one(&mut *tx)
        .await?;
    let after: i64 = outputs
        .iter()
        .map(|o| {
            o.pack
                .as_ref()
                .and_then(|p| p.stored_size)
                .or_else(|| {
                    o.independent
                        .as_ref()
                        .and_then(|c| c.stored_size.map(i64::from))
                })
                .unwrap_or(0)
        })
        .sum();
    let upload: bool = queries::pack_is_upload(task.id).fetch_one(&mut *tx).await?;
    let range: bool = queries::pack_is_range(task.id).fetch_one(&mut *tx).await?;
    if !split && !upload {
        let exclusive: bool = queries::pack_exclusive(
            &ids,
            config::seconds(&settings.repack_cooldown)? as f64,
            config::seconds(&settings.range_repack_window)? as f64,
            flush_seconds,
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure!(exclusive, "pack references changed; retry maintenance");
    }
    for output in outputs.iter().filter(|o| o.pack.is_some()) {
        for pair in output.chunks.windows(2) {
            let a = rows
                .iter()
                .find(|r| r.chunk.id == pair[0])
                .context("missing pack source")?;
            let b = rows
                .iter()
                .find(|r| r.chunk.id == pair[1])
                .context("missing pack source")?;
            ensure!(
                queries::same_references(pair[0], pair[1], b.offset_bytes - a.offset_bytes)
                    .fetch_one(&mut *tx)
                    .await?,
                "pack reference boundaries changed; retry maintenance"
            );
        }
    }
    for output in outputs {
        if let Some(p) = &output.pack {
            queries::publish_prepared_pack(p.id)
                .execute(&mut *tx)
                .await?;
            queries::retire_pack_encodings(&output.chunks)
                .execute(&mut *tx)
                .await?;
        } else if let Some(c) = &output.independent {
            queries::publish_prepared_encoding(c.encoding_id)
                .execute(&mut *tx)
                .await?;
            queries::publish_chunk_encoding(c).execute(&mut *tx).await?;
        }
        queries::publish_pack_layout(
            &output.chunks,
            output.pack.as_ref().map(|p| p.id),
            split,
            config::seconds(if range {
                &settings.range_repack_after
            } else {
                &settings.repack_cooldown
            })? as f64,
            range,
        )
        .execute(&mut *tx)
        .await?;
    }
    let retained: Vec<Id> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    let dead: Vec<Id> = ids
        .iter()
        .filter(|id| !retained.contains(id))
        .copied()
        .collect();
    if !dead.is_empty() {
        ensure!(
            !queries::pack_has_references(&dead)
                .fetch_one(&mut *tx)
                .await?,
            "pack acquired a new reference; retry maintenance"
        );
        queries::fail_dead_pack_chunks(&dead)
            .execute(&mut *tx)
            .await?;
    }
    if split {
        queries::schedule_repacked_streams(
            &retained,
            config::seconds(&settings.repack_cooldown)? as f64,
        )
        .execute(&mut *tx)
        .await?;
    }
    let old: Vec<Id> = rows.iter().filter_map(|r| r.pack_id).collect();
    queries::retire_replaced_packs(old)
        .execute(&mut *tx)
        .await?;
    queries::record_pack_rewrite(task.id, json!({"before_bytes":before,"output_bytes":after,"temporary_added_bytes":added,"transition_bytes":before+added})).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn record_range(
    pool: &PgPool,
    task: Execution,
    id: Id,
    processed: usize,
    decision: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    queries::mark_range_checked(id).execute(&mut *tx).await?;
    queries::record_range_result(task.id, id.to_string(), processed as i64, decision)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn enqueue_task(
    db: &mut PgConnection,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
    child: bool,
) -> Result<Uuid> {
    let id = if child {
        Uuid::new_v4()
    } else {
        crate::model::REQUEST_ID
            .try_with(|id| *id)
            .unwrap_or_else(|_| Uuid::new_v4())
    };
    insert_task(
        db,
        id,
        task_type,
        bucket,
        detail,
        crate::model::audit::current(),
    )
    .await?;
    Ok(id)
}

use crate::model::Mapping;

use crate::content::pack::Pack;
use crate::model::packing::RewriteTask;

pub(super) async fn start_rewrite(
    pool: &PgPool,
    input: &RewriteTask,
    node: Option<Uuid>,
    single: bool,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    queries::lock_rewrite_storage(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
        .context("enabled store was not found")?;
    if let Some(id) = queries::active_rewrite(input.storage_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok((id, true));
    }
    let upper = queries::rewrite_upper(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    if single {
        let detail = json!({"execution_mode":"single","storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
        let id = enqueue_task(&mut tx, "rewrite", None, detail, false).await?;
        if input.shards == 0 {
            queries::complete_empty_rewrite(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        return Ok((id, false));
    }
    let local = queries::rewrite_is_local(input.storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let node = if local { node } else { None };
    let scope = if local { "node" } else { "cluster" };
    let detail = json!({"role":"parent","storage_id":input.storage_id,"upper_chunk_id":upper,"profile_revision":input.profile_revision,"target_key_id":input.target_key_id,"target_key":input.target_key,"target_algorithm":input.target_algorithm});
    let id = enqueue_task(&mut tx, "rewrite", None, detail.clone(), false).await?;
    queries::scope_rewrite(id, scope, node, input.shards)
        .execute(&mut *tx)
        .await?;
    for shard in 0..input.shards {
        let mut child_detail = detail.clone();
        child_detail["role"] = json!("child");
        child_detail["shard"] = json!(shard);
        child_detail["shards"] = json!(input.shards);
        let child = enqueue_task(&mut tx, "rewrite", None, child_detail, true).await?;
        queries::scope_rewrite_child(child, id, scope, node, input.shards)
            .execute(&mut *tx)
            .await?;
    }
    if input.shards == 0 {
        queries::complete_empty_rewrite(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id, false))
}

async fn rewrite_lease(tx: &mut Transaction<'_, Postgres>, task: Uuid, token: Uuid) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task, token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        "rewrite task was paused or lost its lease"
    );
    Ok(())
}

pub(super) async fn commit_rewrite_chunk(
    pool: &PgPool,
    task: Uuid,
    token: Uuid,
    source: &Chunk,
    physical: &Chunk,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    rewrite_lease(&mut tx, task, token).await?;
    let current = queries::lock_rewrite_chunk(source.id)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(current == Some(None), "chunk source changed during rewrite");
    check_source_inputs(&mut tx, task, &[source.id]).await?;
    let changed = queries::retire_rewrite_encoding(source.encoding_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(changed == 1, "source encoding changed during rewrite");
    queries::publish_rewrite_encoding(physical)
        .execute(&mut *tx)
        .await?;
    queries::publish_rewrite_chunk(physical)
        .execute(&mut *tx)
        .await?;
    queries::advance_rewrite(task, source.id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn commit_rewrite_pack(
    pool: &PgPool,
    task: Uuid,
    token: Uuid,
    old: &Pack,
    new: &Pack,
    current: &[Id],
    members: &[Chunk],
    first: Id,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    rewrite_lease(&mut tx, task, token).await?;
    let locked = queries::lock_rewrite_members(old.id)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(locked == current, "pack membership changed during rewrite");
    check_source_inputs(&mut tx, task, current).await?;
    let updated = queries::publish_rewrite_pack(new)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(updated == 1, "new pack was changed during rewrite");
    for (ordinal, member) in members.iter().enumerate() {
        queries::insert_pack_member(
            new.id,
            ordinal as i32,
            member.id,
            members[..ordinal]
                .iter()
                .map(|c| i64::from(c.raw_size))
                .sum::<i64>(),
        )
        .execute(&mut *tx)
        .await?;
    }
    let changed = queries::replace_rewrite_pack(old.id, new.id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(
        changed == current.len() as u64,
        "pack membership changed during rewrite"
    );
    let retired = queries::retire_rewrite_pack(old.id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    ensure!(retired == 1, "source pack changed during rewrite");
    // Packs can interleave chunk IDs; advance only past this pack's scan key.
    queries::advance_rewrite(task, first.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn start_purge(
    pool: &PgPool,
    principal: &Principal,
    name: &str,
    id: Uuid,
    confirmation: Option<&str>,
) -> Result<Value> {
    let mut tx = admin_transaction(pool, principal).await?;
    let found: Option<Uuid> = queries::lock_purge_bucket(name, id)
        .fetch_optional(&mut *tx)
        .await?;
    if found.is_none() {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    if let Some(task) = queries::existing_purge(id).fetch_optional(&mut *tx).await? {
        return Ok(json!({"task_id":task,"existing":true}));
    }
    if let Some(expected) = confirmation {
        let preview = bucket_impact(&mut tx, id).await?;
        if fingerprint(&preview, None)? != expected {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
    }
    queries::seal_purge_bucket(id).execute(&mut *tx).await?;
    let task = enqueue_task(
        &mut tx,
        "purge",
        Some(id),
        json!({"bucket_id":id,"name":name}),
        false,
    )
    .await?;
    checkpoint_event(
        &mut tx,
        "bucket.purge",
        name,
        json!({"bucket_id":id,"task_id":task}),
    )
    .await?;
    tx.commit().await?;
    Ok(json!({"task_id":task}))
}

pub(super) async fn purge_batch(
    pool: &PgPool,
    id: Execution,
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
    active: &[Uuid],
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, id).await?;
    let keys: Vec<(String, Option<Id>)> = queries::lock_purge_objects(bucket, cursor, batch)
        .fetch_all(&mut *tx)
        .await?;
    if !keys.is_empty() {
        let last = &keys.last().unwrap().0;
        for (key, stream) in &keys {
            if let Some(stream) = stream {
                queries::retire_stream(*stream).execute(&mut *tx).await?;
            }
            queries::purge_object(bucket, key, Id::default())
                .execute(&mut *tx)
                .await?;
        }
        queries::advance_task(id.id, last, keys.len() as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_upload_history(bucket, active, batch)
        .execute(&mut *tx)
        .await?;
    let waiting: bool = queries::purge_waiting(bucket).fetch_one(&mut *tx).await?;
    if waiting {
        tx.commit().await?;
        return Ok(false);
    }
    queries::purge_object_rows(bucket).execute(&mut *tx).await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    lock_bucket_dependents(&mut tx, bucket).await?;
    queries::purge_bucket_row(bucket).execute(&mut *tx).await?;
    queries::complete_task(id.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

use crate::model::maintenance::{FragmentCleanup, FragmentDeletion};

pub(super) async fn cleanup_history(
    pool: &PgPool,
    section: &str,
    age: f64,
    batch: i64,
    active: &[Uuid],
    timeout_ms: u128,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    queries::history_limits(&mut tx).await?;
    queries::cleanup_statement_timeout(timeout_ms.max(1).to_string())
        .execute(&mut *tx)
        .await?;
    let count = queries::cleanup_history(section, age, batch, active)?
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(count)
}

pub(super) async fn recover_runtime(pool: &PgPool, maintenance: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::reset_access_coverage().execute(&mut *tx).await?;
    queries::abandon_startup_streams().execute(&mut *tx).await?;
    queries::fail_startup_chunks().execute(&mut *tx).await?;
    queries::retire_startup_encodings()
        .execute(&mut *tx)
        .await?;
    queries::retire_startup_packs().execute(&mut *tx).await?;
    queries::clear_pack_inputs().execute(&mut *tx).await?;
    queries::reset_completing_uploads()
        .execute(&mut *tx)
        .await?;
    queries::reset_startup_tasks(maintenance)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn claim_stale_stream(
    pool: &PgPool,
    id: Id,
    storage: Option<Uuid>,
    retention: Option<f64>,
    single: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    if queries::claim_stale_stream(id, retention, storage, single)
        .build()
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        return Ok(false);
    }
    tx.commit().await?;
    Ok(true)
}
pub(super) async fn cleanup_stream(pool: &PgPool, id: Id, batch: i64) -> Result<usize> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let raw = queries::raw_cleanup_ranges(id, batch.clamp(1, 128))
        .fetch_all(&mut *tx)
        .await?;
    if let Some((last, _)) = raw.last() {
        let mut sources: Vec<_> = raw.iter().map(|(_, source)| *source).collect();
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            queries::raw_source(source).fetch_one(&mut *tx).await?;
            queries::raw_touch_unreferenced(source)
                .execute(&mut *tx)
                .await?;
        }
        queries::raw_detach(id, *last).execute(&mut *tx).await?;
    }
    let candidates = queries::retire_stream_encodings(id, batch)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let detached = queries::detach_stream_chunks(id, batch)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let chunks = queries::remove_stream_extents(id, batch)
        .fetch_all(&mut *tx)
        .await?;
    let count = chunks.len();
    let chunks = chunks.into_iter().flatten().collect::<Vec<_>>();
    mark_unreferenced(&mut tx, &chunks).await?;
    let streams = queries::remove_empty_stream(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(raw.len() + count + (candidates + detached + streams) as usize)
}

struct FragmentDelete {
    tx: Transaction<'static, Postgres>,
    id: Id,
}
#[async_trait::async_trait]
impl FragmentDeletion for FragmentDelete {
    async fn finish(self: Box<Self>) -> Result<()> {
        let Self { mut tx, id } = *self;
        queries::remove_fragment_row(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn claim_fragment(pool: &PgPool, id: Id) -> Result<Option<FragmentCleanup>> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let Some((location_id, sealed, owner, storage, key, file)) = queries::lock_cleanup_fragment(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(FragmentCleanup {
        location_id,
        sealed,
        file,
        owner,
        storage,
        key,
        deletion: Box::new(FragmentDelete { tx, id }),
    }))
}
pub(super) async fn cleanup_tombstones(
    pool: &PgPool,
    storage: Option<Uuid>,
    batch: i64,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    queries::cleanup_limits(&mut tx).await?;
    let count = queries::remove_tombstones(batch, storage)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(count)
}
pub(super) async fn retire_chunk(
    pool: &PgPool,
    id: Id,
    storage: Uuid,
    grace: f64,
    active: &[Id],
) -> Result<Option<Vec<Uuid>>> {
    let mut tx = pool.begin().await?;
    let Some((owner,)) = queries::lock_reclaim_chunk(id, storage, grace)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    if owner.is_some_and(|id| active.contains(&id)) {
        return Ok(None);
    }
    queries::claim_reclaim_chunk(id, storage)
        .execute(&mut *tx)
        .await?;
    let locations = queries::reclaim_locations(id).fetch_all(&mut *tx).await?;
    queries::retire_chunk_encodings(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(locations))
}
pub(super) async fn mark_orphan_chunks(pool: &PgPool) -> Result<usize> {
    let mut tx = pool.begin().await?;
    queries::lock_accounts().execute(&mut *tx).await?;
    let changed = queries::mark_orphan_chunks()
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(changed as usize)
}

use crate::model::integrity::{Detail, Finding};
use crate::worker::integrity::Request as IntegrityRequest;

pub(super) async fn acquire_integrity_rate(
    pool: &PgPool,
    storage_id: Uuid,
    rates: &[(&str, f64, f64)],
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    for &(metric, rate, amount) in rates {
        // A single integrity request/chunk may be larger than one second's
        // configured rate.  Grow the burst capacity for that request so a
        // byte budget can make progress instead of waiting forever.
        let capacity = rate.max(amount).max(1.0);
        queries::initialize_integrity_rate(storage_id, metric, format!("{capacity:.6}"))
            .execute(&mut *tx)
            .await?;
        let (tokens, replenished): (f64, chrono::DateTime<chrono::Utc>) =
            queries::lock_integrity_rate(storage_id, metric)
                .fetch_one(&mut *tx)
                .await?;
        let elapsed = (chrono::Utc::now() - replenished).num_milliseconds().max(0) as f64 / 1000.0;
        let available = (tokens + elapsed * rate).min(capacity);
        if available < amount {
            return Ok(false);
        }
        let remaining = available - amount;
        queries::update_integrity_rate(storage_id, metric, format!("{remaining:.6}"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(true)
}
pub(super) async fn start_integrity(
    pool: &PgPool,
    single: bool,
    input: IntegrityRequest,
    bucket: Option<Bucket>,
    node: Option<Uuid>,
) -> Result<(Uuid, usize)> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy("mokyu:integrity".to_owned())
        .execute(&mut *tx)
        .await?;
    // Limit queued work as well as running work; repeated clicks must not grow an unbounded queue.
    let busy: bool = queries::integrity_busy().fetch_one(&mut *tx).await?;
    if busy {
        return Err(
            s3s::s3_error!(OperationAborted, "an integrity check is already active").into(),
        );
    }
    let storage_rows: Vec<(Uuid, String)> =
        if let Some(storage_id) = bucket.as_ref().and_then(|value| value.storage_id) {
            queries::integrity_storage(storage_id)
                .fetch_all(&mut *tx)
                .await?
        } else if bucket.is_some() {
            Vec::new()
        } else {
            queries::integrity_storages().fetch_all(&mut *tx).await?
        };
    let storage_rows = if storage_rows.is_empty() {
        vec![(Uuid::nil(), "shared".to_owned())]
    } else {
        storage_rows
    };
    let mut details = Vec::with_capacity(storage_rows.len());
    for (storage_id, storage_type) in storage_rows {
        let storage_id = (storage_id != Uuid::nil()).then_some(storage_id);
        let upper: Id = if let Some(storage_id) = storage_id {
            queries::integrity_upper_chunk(storage_id)
                .fetch_one(&mut *tx)
                .await?
        } else {
            queries::integrity_upper_chunk_all()
                .fetch_one(&mut *tx)
                .await?
        };
        let mut detail = Detail {
            storage_id,
            mode: input.mode,
            phase: "metadata".into(),
            bucket_id: bucket.as_ref().map(|value| value.id),
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            upper_chunk_id: upper.to_string(),
            upper_object: None,
            objects_checked: 0,
            chunks_checked: 0,
            bytes_checked: 0,
            issues: 0,
            skipped: 0,
            finished_at: None,
            storages: Vec::new(),
            storage_index: 0,
        };
        detail.upper_object = queries::integrity_upper_object(&mut tx, &detail).await?;
        details.push((storage_type, detail));
    }
    if single {
        let storages = details
            .iter()
            .map(|(_, detail)| crate::model::integrity::StorageSnapshot {
                storage_id: detail.storage_id,
                upper_chunk_id: detail.upper_chunk_id.clone(),
                upper_object: detail.upper_object.clone(),
            })
            .collect();
        let mut detail = details.remove(0).1;
        detail.storages = storages;
        let id = enqueue_task(
            &mut tx,
            "integrity",
            detail.bucket_id,
            serde_json::to_value(detail)?,
            false,
        )
        .await?;
        tx.commit().await?;
        return Ok((id, 0));
    }
    let child_count = details.len();
    let parent_detail = json!({
        "role": "parent",
        "mode": input.mode,
        "bucket": input.bucket.clone(),
        "key": input.key.clone(),
        "children": details.len(),
    });
    let parent = crate::model::REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4());
    insert_task(
        &mut *tx,
        parent,
        "integrity",
        bucket.as_ref().map(|value| value.id),
        parent_detail,
        crate::model::audit::current(),
    )
    .await?;
    for (storage_type, detail) in details {
        let child = Uuid::new_v4();
        insert_task(
            &mut *tx,
            child,
            "integrity",
            detail.bucket_id,
            serde_json::to_value(detail)?,
            crate::model::audit::current(),
        )
        .await?;
        let (execution_scope, target_node_id) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_integrity_task(child, parent, 1, execution_scope, target_node_id)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(parent, child_count as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((parent, child_count))
}
pub(super) async fn checkpoint_integrity(
    pool: &PgPool,
    id: Execution,
    mut cursor: String,
    mut d: Detail,
    issues: Vec<Finding>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, id).await?;
    for i in issues {
        let inserted = queries::insert_integrity_finding(id.id, &i)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        d.issues += inserted;
        if inserted > 0 {
            tracing::warn!(task_id=%id.id,chunk_id=?i.chunk_id,stream_id=?i.stream_id,code=%i.code,"integrity finding");
        }
    }
    let mut advanced = false;
    if d.phase == "done" && !d.storages.is_empty() {
        d.storage_index += 1;
        if let Some(next) = d.storages.get(d.storage_index) {
            d.storage_id = next.storage_id;
            d.upper_chunk_id = next.upper_chunk_id.clone();
            d.upper_object = next.upper_object.clone();
            d.phase = "metadata".into();
            d.finished_at = None;
            cursor = serde_json::to_string(&crate::model::integrity::Cursor::default())?;
            advanced = true;
        }
    }
    queries::integrity_checkpoint(
        id.id,
        cursor,
        serde_json::to_value(&d)?,
        (d.objects_checked + d.chunks_checked) as i64,
        d.phase == "done",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if advanced {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("integrity-storage-advanced").await;
    }
    Ok(())
}

pub(super) async fn change_pack_creation(pool: &PgPool, paused: bool) -> Result<()> {
    if !paused {
        let unpacking: bool = queries::unpacking_all().fetch_one(pool).await?;
        if unpacking {
            return Err(s3s::s3_error!(
                OperationAborted,
                "finish all-pack unpacking before enabling new packs"
            )
            .into());
        }
    }
    let mut tx = pool.begin().await?;
    queries::pause_pack_creation(paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.pack_creation",
        "pack",
        json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;

    Ok(())
}
pub(super) async fn change_maintenance_policy(
    pool: &PgPool,
    policy: &str,
    paused: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    if policy == "gc" {
        queries::pause_gc(paused).execute(&mut *tx).await?;
    }
    queries::pause_maintenance_policy(policy, paused)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "maintenance.policy",
        policy,
        json!({"paused":paused}),
    )
    .await?;
    tx.commit().await?;

    Ok(())
}
pub(super) async fn start_lifecycle_policy(
    pool: &PgPool,
    single: bool,
    policy: &str,
    node: Option<Uuid>,
) -> Result<(Uuid, bool)> {
    let mut tx = pool.begin().await?;
    queries::lock_pack_policy(format!("mokyu:maintenance:{policy}"))
        .execute(&mut *tx)
        .await?;
    if let Some(id) = queries::active_lifecycle_task(policy)
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok((id, true));
    }
    let storages: Vec<(Uuid, String, i64)> =
        queries::lifecycle_storages().fetch_all(&mut *tx).await?;
    if single {
        let detail = json!({"phase":if policy == "cleanup" {"history"} else {"storage"}});
        let id = enqueue_storage_task(&mut tx, policy, detail, &storages).await?;
        queries::scheduled_maintenance(policy)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok((id, false));
    }
    let children = storages.len() + usize::from(policy == "cleanup");
    let id = enqueue_task(
        &mut tx,
        policy,
        None,
        json!({"role":"parent","children":children}),
        false,
    )
    .await?;
    if policy == "cleanup" {
        let child = enqueue_task(
            &mut tx,
            policy,
            None,
            json!({"role":"child","phase":"history"}),
            true,
        )
        .await?;
        queries::scope_history_task(child, id)
            .execute(&mut *tx)
            .await?;
    }
    for (storage_id, storage_type, revision) in &storages {
        let child = enqueue_task(
            &mut tx,
            policy,
            None,
            json!({"role":"child","storage_id":storage_id,"profile_revision":revision}),
            true,
        )
        .await?;
        let (scope, target_node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, target_node)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(id, children as i64)
        .execute(&mut *tx)
        .await?;
    if children == 0 {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    queries::scheduled_maintenance(policy)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok((id, false))
}

pub(super) async fn finish_single_lifecycle(
    pool: &PgPool,
    task: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut detail = queries::lock_task_detail(task)
        .fetch_optional(&mut *tx)
        .await?
        .context("task execution expired")?;
    if let Some(storage) = detail["storage_id"].as_str().map(str::to_owned) {
        let saved = &mut detail["report"]["storages"][storage];
        for (key, value) in report.as_object().context("invalid maintenance report")? {
            if let Some(count) = value.as_i64() {
                saved[key] = json!(
                    saved[key]
                        .as_i64()
                        .unwrap_or(0)
                        .checked_add(count)
                        .context("maintenance report overflow")?
                );
            }
        }
    } else {
        detail["report"]["history"] = report;
    }
    let done = !again && advance_storage_scope(&mut detail)?;
    queries::advance_storage_task(task.id, detail, count, done)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if !again && !done {
        #[cfg(feature = "fault-injection")]
        crate::faults::point("lifecycle-storage-advanced").await;
    }
    Ok(())
}

use crate::model::object::{ObjectTransfer, Outcome, RECEIPT, StorageMigration};

async fn complete_media_operation(
    tx: &mut Transaction<'_, Postgres>,
    version: Option<Id>,
) -> Result<()> {
    record_media_operation(
        tx,
        Outcome {
            status: 200,
            code: None,
            output_version: version,
            task_id: None,
        },
    )
    .await
}

async fn queue_media_operation(tx: &mut Transaction<'_, Postgres>, task_id: Uuid) -> Result<()> {
    record_media_operation(
        tx,
        Outcome {
            status: 202,
            code: None,
            output_version: None,
            task_id: Some(task_id),
        },
    )
    .await
}

async fn record_media_operation(tx: &mut Transaction<'_, Postgres>, result: Outcome) -> Result<()> {
    if let Ok((id, bucket, key, action, count)) =
        RECEIPT.try_with(|r| (r.id, r.bucket, r.key.clone(), r.action, r.count))
    {
        let changed = queries::save_media_operation(id, &result)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        ensure!(changed == 1, "media operation receipt is missing");
        checkpoint_event(
            tx,
            "object.change",
            &key,
            json!({"bucket_id":bucket,"operation":action,"version":result.output_version,"task_id":result.task_id,"operation_id":id}),
        )
        .await?;
        if count > 1
            && let Some(id) = crate::model::audit::current()
        {
            partial_audit(tx, id).await?;
        }
    }
    Ok(())
}

async fn lock_media_scope(tx: &mut Transaction<'_, Postgres>, permit: &Permit) -> Result<()> {
    permit.lock(tx).await?;
    let buckets: Vec<Uuid> = permit.resources.keys().copied().collect();
    let blocked: bool = queries::blocked_media_buckets(&buckets)
        .fetch_one(&mut **tx)
        .await?;
    if blocked {
        return Err(s3s::s3_error!(OperationAborted).into());
    }
    queries::lock_media_projects(&buckets)
        .execute(&mut **tx)
        .await?;
    queries::media_bucket_scopes(&buckets)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn selected_object(
    tx: &mut Transaction<'_, Postgres>,
    bucket: Uuid,
    key: &str,
    expected: Option<Id>,
) -> Result<Option<Id>> {
    let actual: Option<Id> = queries::lock_object_stream(bucket, key)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
    if actual != expected {
        return Err(s3s::s3_error!(PreconditionFailed).into());
    }
    Ok(actual)
}

pub(super) async fn begin_media_operation(
    pool: &PgPool,
    principal: &Principal,
    user: Uuid,
    client: Uuid,
    hash: &[u8],
) -> Result<(Uuid, Vec<u8>)> {
    let mut tx = pool.begin().await?;
    principal.lock_identity(&mut tx).await?;
    queries::insert_media_operation(user, principal.token_id(), client, hash)
        .execute(&mut *tx)
        .await?;
    let (id, expected): (Uuid, Vec<u8>) =
        queries::find_media_operation(user, principal.token_id(), client)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok((id, expected))
}

pub(super) async fn audit_object_batch(
    pool: &PgPool,
    bucket: Uuid,
    detail: Value,
    partial: bool,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    checkpoint_event(&mut tx, "object.batch", &bucket.to_string(), detail).await?;
    if partial && let Some(id) = crate::model::audit::current() {
        partial_audit(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn move_object(
    pool: &PgPool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
) -> Result<()> {
    let target = input.target_bucket;
    let mut tx = pool.begin().await?;
    lock_media_scope(&mut tx, permit).await?;
    selected_object(
        &mut tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    let key = input.target_key;
    let old = selected_object(&mut tx, target, key, input.target_version).await?;
    let public: bool = queries::stream_public(input.source_version)
        .fetch_one(&mut *tx)
        .await?;
    if public {
        Permit::for_action(permit.principal.clone(), target, Action::Acl)
            .lock(&mut tx)
            .await?;
    }
    queries::tombstone_object(input.source_bucket, input.source_key, Uuid::new_v4())
        .execute(&mut *tx)
        .await?;
    queries::move_stream(input.source_version, target, key)
        .execute(&mut *tx)
        .await?;
    queries::upsert_object(target, key, input.source_version)
        .execute(&mut *tx)
        .await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut *tx).await?;
    }
    complete_media_operation(&mut tx, Some(input.source_version)).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn publish_copy(
    pool: &PgPool,
    permit: &Permit,
    input: ObjectTransfer<'_>,
    id: Id,
    public: bool,
    expected_public: Option<bool>,
) -> Result<()> {
    let (target, key, expected) = (input.target_bucket, input.target_key, input.target_version);
    let mut tx = pool.begin().await?;
    let mut authority = permit.clone();
    if public {
        authority.add(target, Action::Acl);
    }
    lock_media_scope(&mut tx, &authority).await?;
    selected_object(
        &mut tx,
        input.source_bucket,
        input.source_key,
        Some(input.source_version),
    )
    .await?;
    let old = selected_object(&mut tx, target, key, expected).await?;
    if let Some(expected_public) = expected_public {
        let current: bool = queries::stream_public(input.source_version)
            .fetch_one(&mut *tx)
            .await?;
        if current != expected_public {
            return Err(s3s::s3_error!(PreconditionFailed).into());
        }
    }
    require_writing_stream(&mut tx, id).await?;
    let (_, _, _, size, payload) = queries::lock_writing_stream(id).fetch_one(&mut *tx).await?;
    if payload == "raw" {
        validate_raw(&mut tx, id, size).await?;
    } else {
        require_sources(&mut tx, Some(id), &[]).await?;
    }
    queries::publish_copy_stream(id).execute(&mut *tx).await?;
    queries::upsert_object(target, key, id)
        .execute(&mut *tx)
        .await?;
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut *tx).await?;
    }
    complete_media_operation(&mut tx, Some(id)).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn queue_storage_migration(
    pool: &PgPool,
    permit: &Permit,
    job: &StorageMigration,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut authority = permit.clone();
    if job.public_read {
        authority.add(job.target_bucket, Action::Acl);
    }
    lock_media_scope(&mut tx, &authority).await?;
    selected_object(
        &mut tx,
        job.source_bucket,
        &job.source_key,
        Some(job.source_version),
    )
    .await?;
    selected_object(
        &mut tx,
        job.target_bucket,
        &job.target_key,
        job.target_version,
    )
    .await?;
    let task = enqueue_task(
        &mut tx,
        "storage_migration",
        Some(job.target_bucket),
        serde_json::to_value(job)?,
        false,
    )
    .await?;
    queue_media_operation(&mut tx, task).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn complete_storage_migration(
    pool: &PgPool,
    id: Uuid,
    lease_token: Uuid,
    destination: Id,
    job: &StorageMigration,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let owned: Option<Uuid> = queries::lock_migration_task(id, lease_token)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        owned.is_some(),
        "storage migration lease lost before publish"
    );
    let source_current = selected_object(
        &mut tx,
        job.source_bucket,
        &job.source_key,
        Some(job.source_version),
    )
    .await?;
    let old = selected_object(
        &mut tx,
        job.target_bucket,
        &job.target_key,
        job.target_version,
    )
    .await?;
    ensure!(
        source_current == Some(job.source_version),
        "source object changed during storage migration"
    );
    require_writing_stream(&mut tx, destination).await?;
    let (_, _, _, size, payload) = queries::lock_writing_stream(destination)
        .fetch_one(&mut *tx)
        .await?;
    if payload == "raw" {
        validate_raw(&mut tx, destination, size).await?;
    } else {
        require_sources(&mut tx, Some(destination), &[]).await?;
    }
    let published = queries::publish_migration_stream(destination, id, lease_token)
        .execute(&mut *tx)
        .await?;
    ensure!(
        published.rows_affected() == 1,
        "storage migration stream was reclaimed"
    );
    queries::upsert_object(job.target_bucket, &job.target_key, destination)
        .execute(&mut *tx)
        .await?;
    if job.action == "move" {
        queries::tombstone_object(job.source_bucket, &job.source_key, Uuid::new_v4())
            .execute(&mut *tx)
            .await?;
        queries::retire_stream(job.source_version)
            .execute(&mut *tx)
            .await?;
    }
    if let Some(old) = old {
        queries::retire_stream(old).execute(&mut *tx).await?;
    }
    let completed = queries::complete_migration_task(id, lease_token)
        .execute(&mut *tx)
        .await?;
    ensure!(
        completed.rows_affected() == 1,
        "storage migration lease lost before publish"
    );
    tx.commit().await?;
    Ok(())
}

pub(super) async fn browse_catalog(
    pool: &PgPool,
    authority: &Permit,
    bucket: Uuid,
    filter: &mut crate::model::object::CatalogFilter,
    cursor: Option<crate::model::object::CatalogCursor>,
    sizes: (Option<i64>, Option<i64>),
) -> Result<Vec<crate::model::object::MediaItem>> {
    let mut tx = pool.begin().await?;
    queries::catalog_search_settings().execute(&mut *tx).await?;
    authority.lock(&mut tx).await?;
    let rows = queries::browse_catalog(&mut tx, bucket, filter, cursor, sizes).await?;
    tx.commit().await?;
    Ok(rows)
}

use crate::model::quota::{Limits, Quota, number, validate_scope};

async fn read_quota(pool: &PgPool, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
    validate_scope(scope)?;
    let mut tx = pool.begin().await?;
    let admin = principal.lock_identity(&mut tx).await?;
    let visible = if scope == "bucket" {
        Permit::for_action(principal.clone(), id, Action::Inspect)
            .lock(&mut tx)
            .await?;
        true
    } else if admin {
        true
    } else {
        if principal.token_id().is_some() {
            return Err(s3s::s3_error!(AccessDenied).into());
        }
        let scope: Option<String> = queries::quota_membership(principal.user_id(), id)
            .fetch_optional(&mut *tx)
            .await?;
        scope.ok_or_else(|| s3s::s3_error!(AccessDenied))? == "all"
    };
    let result = queries::quota_scope(scope, id, visible)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(result)
}
async fn save_quota(
    pool: &PgPool,
    principal: &Principal,
    scope: &str,
    id: Uuid,
    input: Limits,
) -> Result<Quota> {
    validate_scope(scope)?;
    let (bytes, inflight, buckets) = (
        number(&input.logical_bytes_limit)?,
        number(&input.inflight_bytes_limit)?,
        number(&input.bucket_count_limit)?,
    );
    if scope == "bucket" && (inflight.is_some() || buckets.is_some()) {
        return Err(s3s::s3_error!(InvalidArgument).into());
    }
    let mut tx = admin_transaction(pool, principal).await?;
    let changed = queries::update_quota(scope, id, bytes, inflight, buckets)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed != 1 {
        return Err(s3s::s3_error!(NoSuchKey).into());
    }
    let mut detail = serde_json::to_value(input)?;
    detail[format!("{scope}_id")] = serde_json::json!(id);
    checkpoint_event(&mut tx, "quota.update", &id.to_string(), detail).await?;
    tx.commit().await?;
    read_quota(pool, principal, scope, id).await
}

use crate::model::metrics::{
    AccessBatch, CapacityPoint, RuntimeHistory, RuntimePoint, ScopeQuery, StorageUsage,
    UsageSnapshot,
};

use queries::SavedStorage;

pub(super) async fn collect_inventory(
    pool: &PgPool,
    metrics: &config::MetricsSettings,
    maintenance: &config::Maintenance,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::inventory_isolation().execute(&mut *tx).await?;
    // One worker, bounded working memory and wait time; large sorts may spill to PostgreSQL disk.
    queries::inventory_limits().execute(&mut *tx).await?;
    let timeout = config::seconds(&metrics.query_timeout)? * 1000;
    queries::inventory_timeout(timeout.to_string())
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = queries::inventory_time().fetch_one(&mut *tx).await?;
    let buckets: Vec<Value> = queries::inventory_buckets().fetch_all(&mut *tx).await?;
    let chunks: Value = queries::inventory_chunks().fetch_one(&mut *tx).await?;
    let physical: Value = queries::inventory_physical().fetch_one(&mut *tx).await?;
    let mut chunks = chunks;
    chunks["stored_bytes"] = json!(
        physical
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| matches!(p["state"].as_str(), Some("ready" | "retired" | "deleting")))
            .map(|p| p["stored_bytes"].as_i64().unwrap_or(0))
            .sum::<i64>()
    );
    chunks["unconfirmed_bytes"] = json!(
        physical
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| matches!(p["state"].as_str(), Some("preparing" | "uploading")))
            .map(|p| p["stored_bytes"].as_i64().unwrap_or(0))
            .sum::<i64>()
    );
    let live: Value = queries::inventory_live().fetch_one(&mut *tx).await?;
    let mut unreferenced: Value = queries::inventory_unreferenced()
        .fetch_one(&mut *tx)
        .await?;
    let reclaimable: Value = queries::inventory_reclaimable().fetch_one(&mut *tx).await?;
    unreferenced["stored_bytes"] = reclaimable["bytes"].clone();
    unreferenced["eligible_bytes"] = reclaimable["eligible"].clone();
    let tasks: Value = queries::inventory_tasks().fetch_one(&mut *tx).await?;
    let uploads: Value = queries::inventory_uploads().fetch_one(&mut *tx).await?;
    let cleanup: Value =
        queries::inventory_cleanup(config::seconds(&maintenance.task_retention)? as f64)
            .fetch_one(&mut *tx)
            .await?;
    let database: Vec<Value> = queries::inventory_database().fetch_all(&mut *tx).await?;
    collect_usage(&mut tx, metrics).await?;
    tx.commit().await?;
    Ok(
        json!({"as_of":as_of,"collected_at":Utc::now(),"objects":buckets[0]["objects"],
        "logical_bytes":buckets[0]["logical_bytes"],"buckets":buckets.iter().skip(1).take(1000).collect::<Vec<_>>(),
        "buckets_truncated":buckets.len()>1001,"chunks":chunks,"live":live,"physical":physical,
        "unreferenced":unreferenced,"tasks":tasks,"uploads":uploads,"cleanup":cleanup,"database":database}),
    )
}

pub(super) async fn collect_storage(pool: &PgPool, storage_id: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    queries::storage_inventory_limits()
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = queries::inventory_time().fetch_one(&mut *tx).await?;
    let buckets: (i64, i64, i64) = queries::storage_inventory_buckets(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let chunks: (i64, i64) = queries::storage_inventory_chunks(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    let physical: (i64, i64) = queries::storage_inventory_physical(storage_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(
        json!({"storage_id":storage_id,"as_of":as_of,"buckets":buckets.0,"objects":buckets.1,"logical_bytes":buckets.2,"chunks":chunks.0,"stored_bytes":chunks.1,"physical":{"objects":physical.0,"stored_bytes":physical.1}}),
    )
}

pub(super) async fn start_usage(pool: &PgPool, node: Option<Uuid>) -> Result<Value> {
    let mut tx = pool.begin().await?;
    if let Some(id) = queries::active_usage_task()
        .fetch_optional(&mut *tx)
        .await?
    {
        tx.commit().await?;
        return Ok(json!({"task_id":id,"existing":true}));
    }
    let storages: Vec<(Uuid, String, i64)> =
        queries::lifecycle_storages().fetch_all(&mut *tx).await?;
    let id = enqueue_task(
        &mut *tx,
        "usage",
        None,
        json!({"role":"parent","children":storages.len()}),
        false,
    )
    .await?;
    for (storage_id, storage_type, revision) in &storages {
        let child = enqueue_task(
            &mut *tx,
            "usage",
            None,
            json!({"role":"child","storage_id":storage_id,"profile_revision":revision}),
            true,
        )
        .await?;
        let (scope, target_node) = if storage_type == "local" {
            ("node", node)
        } else {
            ("cluster", None)
        };
        queries::scope_pack_task(child, id, scope, target_node)
            .execute(&mut *tx)
            .await?;
    }
    queries::task_batch_total(id, storages.len() as i64)
        .execute(&mut *tx)
        .await?;
    if storages.is_empty() {
        queries::complete_task(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(json!({"task_id":id}))
}

// Every scope is derived from live permissions; a caller cannot submit a cached scope ID.
async fn usage_scope(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    query: &ScopeQuery,
) -> Result<(Vec<Uuid>, bool)> {
    let admin = principal.lock_identity(tx).await?;
    let ids: Vec<Uuid> = queries::usage_scope_buckets(
        principal.user_id(),
        admin,
        principal.token_id(),
        query.bucket,
        query.project,
    )
    .fetch_all(&mut **tx)
    .await?;
    if query.bucket.is_some() && ids.is_empty() {
        return Err(s3s::s3_error!(AccessDenied).into());
    }
    Ok((ids, admin))
}

pub(super) async fn read_usage(
    pool: &PgPool,
    principal: &Principal,
    query: &ScopeQuery,
    limit: usize,
) -> Result<UsageSnapshot> {
    let mut tx = pool.begin().await?;
    let (ids, admin) = usage_scope(&mut tx, principal, query).await?;
    let global = admin && query.bucket.is_none() && query.project.is_none();
    let key = if global {
        "global".to_owned()
    } else {
        blake3::hash(&serde_json::to_vec(&ids)?)
            .to_hex()
            .to_string()
    };
    let exists: bool = queries::usage_exists(&key).fetch_one(&mut *tx).await?;
    if !exists {
        queries::usage_scope_lock().execute(&mut *tx).await?;
        // Bound cached permission combinations, including workloads from short-lived tokens.
        queries::usage_scope_evict(limit as i64 - 1)
            .execute(&mut *tx)
            .await?;
        queries::usage_scope_create(&key, &ids)
            .execute(&mut *tx)
            .await?;
    }
    queries::usage_scope_touch(&key).execute(&mut *tx).await?;
    let row: Option<SavedStorage> = queries::usage_saved(&key).fetch_optional(&mut *tx).await?;
    let SavedStorage { as_of, data } = row.unwrap_or_default();
    let points: Vec<(DateTime<Utc>, sqlx::types::Json<StorageUsage>)> =
        queries::usage_history(&key).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(UsageSnapshot {
        global,
        bucket_count: ids.len(),
        as_of,
        usage: data.map(|v| v.0),
        history: points
            .into_iter()
            .map(|(at, data)| CapacityPoint { at, usage: data.0 })
            .collect(),
        created: !exists,
    })
}

async fn collect_usage(
    tx: &mut Transaction<'_, Postgres>,
    metrics: &config::MetricsSettings,
) -> Result<()> {
    queries::usage_scope_expire(config::seconds(&metrics.storage_retention)? as f64)
        .execute(&mut **tx)
        .await?;
    queries::collect_usage().execute(&mut **tx).await?;
    queries::usage_history_append(config::seconds(&metrics.storage_interval)? as f64)
        .execute(&mut **tx)
        .await?;
    queries::usage_history_expire(config::seconds(&metrics.storage_retention)? as f64)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn read_runtime(pool: &PgPool, principal: &Principal) -> Result<RuntimeHistory> {
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    principal.lock_admin(&mut tx).await?;
    let history: Vec<sqlx::types::Json<RuntimePoint>> =
        queries::runtime_history().fetch_all(&mut *tx).await?;
    let work = queries::runtime_work().fetch_all(&mut *tx).await?;
    let (pending_entries, pending_bytes, oldest_pending, failed_uploads): (
        String,
        String,
        Option<DateTime<Utc>>,
        String,
    ) = queries::runtime_pending().fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(RuntimeHistory {
        history: history.into_iter().map(|v| v.0).collect(),
        work,
        pending_entries,
        pending_bytes,
        oldest_pending,
        failed_uploads,
    })
}

pub(super) async fn sample_runtime(
    pool: &PgPool,
    point: RuntimePoint,
    retention: f64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::runtime_sample(point.at, sqlx::types::Json(point))
        .execute(&mut *tx)
        .await?;
    queries::runtime_expire(retention).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn flush_access(pool: &PgPool, pending: AccessBatch, stale: f64) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Each capped map is persisted in one bulk query.
    let mut ids = Vec::new();
    let mut hours = Vec::new();
    let mut reads = Vec::new();
    let mut ranges = Vec::new();
    let mut bytes = Vec::new();
    let mut origins = Vec::new();
    for ((id, hour), c) in pending.chunks {
        ids.push(id);
        hours.push(hour);
        reads.push(c.reads);
        ranges.push(c.ranges);
        bytes.push(c.bytes);
        origins.push(c.origins);
    }
    queries::access_chunks(&ids, &reads, &ranges, &bytes)
        .execute(&mut *tx)
        .await?;
    queries::access_chunk_windows(&ids, hours, reads, ranges, bytes, origins)
        .execute(&mut *tx)
        .await?;
    let mut ids = Vec::new();
    let mut hours = Vec::new();
    let mut reads = Vec::new();
    let mut partial = Vec::new();
    let mut partial_bytes = Vec::new();
    let mut bytes = Vec::new();
    let mut useful = Vec::new();
    for ((id, hour), p) in pending.packs {
        ids.push(id);
        hours.push(hour);
        reads.push(p.downloads);
        partial.push(p.partial);
        partial_bytes.push(p.partial_bytes);
        bytes.push(p.bytes);
        useful.push(p.useful);
    }
    queries::access_packs(ids, hours, reads, bytes, partial, useful, partial_bytes)
        .execute(&mut *tx)
        .await?;
    let (mut packs, mut chunks, mut hours, mut counts) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for ((p, c, h), n) in pending.members {
        packs.push(p);
        chunks.push(c);
        hours.push(h);
        counts.push(n);
    }
    queries::access_members(packs, chunks, hours, counts)
        .execute(&mut *tx)
        .await?;
    queries::access_coverage(pending.dropped, stale)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

use crate::model::packing::{PackMember, PackObject, PackSummary};

async fn scoped_pack_limits(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    queries::scoped_pack_limits().execute(&mut **tx).await?;
    Ok(())
}

pub(super) async fn scoped_pack_page(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    after: Id,
    limit: i64,
    state: Option<&str>,
) -> Result<Vec<PackSummary>> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let all = admin && scope.bucket.is_none() && scope.project.is_none();
    let rows: Vec<PackSummary> = queries::scoped_packs(after, &ids, admin, all, state, limit)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

pub(super) async fn scoped_pack_detail(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    after: Id,
    limit: i64,
) -> Result<(PackSummary, Vec<PackMember>, bool)> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let all = admin && scope.bucket.is_none() && scope.project.is_none();
    let pack: PackSummary = queries::scoped_pack(id, &ids, admin, all)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let members: Vec<PackMember> = queries::scoped_pack_members(id, &ids, admin, all, after, limit)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((pack, members, !admin))
}

pub(super) async fn scoped_pack_objects(
    pool: &PgPool,
    principal: &Principal,
    scope: &ScopeQuery,
    id: Id,
    cursor: Option<(Uuid, String)>,
) -> Result<Vec<PackObject>> {
    let mut tx = pool.begin().await?;
    scoped_pack_limits(&mut tx).await?;
    let (ids, admin) = usage_scope(&mut tx, principal, scope).await?;
    let rows: Vec<PackObject> = queries::scoped_pack_objects(
        id,
        ids,
        principal.user_id(),
        principal.token_id(),
        admin,
        cursor.as_ref().map(|v| v.0),
        cursor.as_ref().map(|v| &v.1),
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

struct AdminAccess {
    tx: Transaction<'static, Postgres>,
}
#[async_trait::async_trait]
impl crate::model::AdminGuard for AdminAccess {
    async fn checkpoint(&mut self, action: &str, target: &str, detail: Value) -> Result<()> {
        checkpoint_event(&mut self.tx, action, target, detail).await
    }
    async fn finish(self: Box<Self>) -> Result<()> {
        self.tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn hold_admin(
    pool: &PgPool,
    principal: &Principal,
) -> Result<Box<dyn crate::model::AdminGuard>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    Ok(Box::new(AdminAccess { tx }))
}

use crate::model::maintenance::{MaintenanceCompletion, MaintenanceExecution, MaintenancePreview};

pub(super) async fn create_maintenance_preview(
    pool: &PgPool,
    principal: &Principal,
    action: &str,
    parameters: Value,
    impact: Value,
    confirmation: String,
) -> Result<MaintenancePreview> {
    let mut tx = pool.begin().await?;
    // One bounded, expiring row replaces a caller's previous unexecuted preview of this action.
    queries::remove_maintenance_preview(principal.user_id(), principal.token_id(), action)
        .execute(&mut *tx)
        .await?;
    let fingerprint = blake3::hash(&serde_json::to_vec(&impact)?)
        .to_hex()
        .to_string();
    let (id, expires_at) = queries::create_maintenance_preview(
        principal.user_id(),
        principal.token_id(),
        action,
        json!({"input":parameters,"confirmation":confirmation,"impact":impact}),
        fingerprint,
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(MaintenancePreview {
        id,
        confirmation,
        expires_at,
        impact,
    })
}

struct MaintenanceReceipt {
    tx: Transaction<'static, Postgres>,
    id: Uuid,
    action: String,
    recorded: Option<Uuid>,
}
#[async_trait::async_trait]
impl MaintenanceCompletion for MaintenanceReceipt {
    async fn finish(mut self: Box<Self>, task: Uuid, audit: bool) -> Result<()> {
        if self.recorded != Some(task) {
            queries::record_maintenance_preview(self.id, task)
                .execute(&mut *self.tx)
                .await?;
        }
        if audit {
            checkpoint_event(
                &mut self.tx,
                &format!("maintenance.{}", self.action),
                &task.to_string(),
                json!({"preview_id":self.id}),
            )
            .await?;
        }
        self.tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn maintenance_execution(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    action: &str,
) -> Result<Option<MaintenanceExecution>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let saved: Option<(Value, String, Option<Uuid>)> =
        queries::lock_maintenance_preview(id, principal.user_id(), principal.token_id(), action)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((parameters, fingerprint, mut task)) = saved else {
        return Ok(None);
    };
    let recorded = task;
    if task.is_none()
        && queries::maintenance_preview_task(id)
            .fetch_one(&mut *tx)
            .await?
    {
        task = Some(id);
    }
    Ok(Some(MaintenanceExecution {
        parameters,
        fingerprint,
        task,
        completion: Box::new(MaintenanceReceipt {
            tx,
            id,
            action: action.to_owned(),
            recorded,
        }),
    }))
}

pub(super) async fn task_page(
    pool: &PgPool,
    principal: &Principal,
    filter: crate::model::task::JobQuery,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    queries::task_page_limits().execute(&mut *tx).await?;
    let values = queries::task_page(filter, cursor, limit)
        .build_query_scalar()
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(values)
}
pub(super) async fn task_view(pool: &PgPool, principal: &Principal, task: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    principal.lock_admin(&mut tx).await?;
    let value = queries::task_view(task)
        .build_query_scalar()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(value)
}

pub(super) async fn maintenance_mode(pool: &PgPool, enabled: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::set_maintenance_mode(enabled)
        .execute(&mut *tx)
        .await?;
    if enabled {
        queries::pause_maintenance_tasks().execute(&mut *tx).await?;
    }
    checkpoint_event(
        &mut tx,
        "maintenance.mode",
        "deployment",
        json!({"enabled":enabled}),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

use crate::model::storage::{StorageCreation, StorageEdit, StorageInitializer, StorageUpdate};
use crate::storage::{
    model::Record as BackendRecord,
    profile::{StorageProfile, validate as validate_storage_profile},
};

async fn save_storage_profile(
    tx: &mut Transaction<'_, Postgres>,
    profile: &StorageProfile,
) -> Result<()> {
    let affected = queries::save_storage_profile(profile)?
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if affected != 1 {
        return Err(crate::model::Error::StorageProfileConflict.into());
    }
    Ok(())
}
async fn validate_block_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let Some(key_id) = key_id else {
        return Ok(());
    };
    let valid: bool = queries::valid_storage_key(key_id)
        .fetch_one(&mut **tx)
        .await?;
    anyhow::ensure!(valid, "active block key was not found");
    Ok(())
}

async fn lock_mode(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<(Uuid, String)> {
    Ok(queries::storage_mode_lock().fetch_one(&mut **tx).await?)
}

async fn load_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
) -> anyhow::Result<crate::model::storage::StorageRow> {
    Ok(queries::storage_row_lock(name)
        .fetch_optional(&mut **tx)
        .await?
        .context("storage was not found")?)
}

pub(super) async fn update_storage_profile(
    pool: &PgPool,
    name: String,
    profile: StorageProfile,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin().await?;
    let (storage_id, storage_type, key): (Uuid, String, Option<Uuid>) =
        queries::storage_profile_target(&name)
            .fetch_optional(&mut *tx)
            .await?
            .context("storage was not found")?;
    anyhow::ensure!(
        profile.storage_id == storage_id,
        "storage profile belongs to another storage"
    );
    validate_storage_profile(&profile, &storage_type)?;
    profile.validate_key(key)?;
    save_storage_profile(&mut tx, &profile).await?;
    checkpoint_event(
        &mut tx,
        "storage.profile.update",
        &name,
        serde_json::json!({"revision": profile.revision + 1}),
    )
    .await?;
    queries::notify_storage_profile(storage_id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(storage_id)
}

struct StorageInitialization {
    tx: Transaction<'static, Postgres>,
    id: Option<Uuid>,
}
#[async_trait::async_trait]
impl StorageInitializer for StorageInitialization {
    async fn prepare(
        &mut self,
        record: &BackendRecord,
        key: Option<Uuid>,
        read: Option<i32>,
        upload: Option<i32>,
        control: Option<i32>,
        profile: Option<StorageProfile>,
    ) -> Result<StorageProfile> {
        anyhow::ensure!(self.id.is_none(), "storage is already prepared");
        validate_block_key(&mut self.tx, key).await?;
        queries::insert_storage_record(record, key)
            .execute(&mut *self.tx)
            .await?;
        queries::insert_storage_profile(record.id, read, upload, control, record.r#type == "s3")
            .execute(&mut *self.tx)
            .await?;
        if let Some(mut profile) = profile {
            profile.storage_id = record.id;
            profile.revision = 1;
            validate_storage_profile(&profile, &record.r#type)?;
            profile.validate_key(key)?;
            save_storage_profile(&mut self.tx, &profile).await?;
        }
        let effective = queries::storage_profile(record.id)
            .fetch_one(&mut *self.tx)
            .await?;
        validate_storage_profile(&effective, &record.r#type)?;
        self.id = Some(record.id);
        Ok(effective)
    }
    async fn finish(mut self: Box<Self>) -> Result<()> {
        let id = self.id.context("storage was not prepared")?;
        queries::notify_storage_profile(id.to_string())
            .execute(&mut *self.tx)
            .await?;
        self.tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn storage_creation(pool: &PgPool) -> Result<StorageCreation> {
    let mut tx = pool.begin().await?;
    let (deployment, mode) = lock_mode(&mut tx).await?;
    Ok(StorageCreation {
        deployment,
        mode,
        writer: Box::new(StorageInitialization { tx, id: None }),
    })
}

struct StorageModification {
    original_type: String,
    tx: Transaction<'static, Postgres>,
}
pub(super) async fn storage_edit(pool: &PgPool, name: &str) -> Result<StorageEdit> {
    let mut tx = pool.begin().await?;
    let (_, mode) = lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, name).await?;
    let profile: StorageProfile = queries::storage_profile_lock(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let original_type = row.r#type.clone();
    Ok(StorageEdit {
        row,
        profile,
        mode,
        update: Box::new(StorageModification { tx, original_type }),
    })
}
#[async_trait::async_trait]
impl StorageUpdate for StorageModification {
    async fn finish(
        mut self: Box<Self>,
        record: &BackendRecord,
        key: Option<Uuid>,
        profile: &StorageProfile,
    ) -> Result<()> {
        validate_storage_profile(profile, &record.r#type)?;
        profile.validate_key(key)?;
        if self.original_type != record.r#type {
            ensure!(
                queries::storage_relocation_ready()
                    .fetch_one(&mut *self.tx)
                    .await?,
                crate::model::Error::StorageNotDrained
            );
            queries::relocate_raw_sources(
                record.id,
                if record.r#type == "local" {
                    "local"
                } else {
                    "shared_s3"
                },
            )
            .execute(&mut *self.tx)
            .await?;
        }
        validate_block_key(&mut self.tx, key).await?;
        queries::update_storage_record(record, key)
            .execute(&mut *self.tx)
            .await?;
        save_storage_profile(&mut self.tx, profile).await?;
        queries::notify_storage_profile(record.id.to_string())
            .execute(&mut *self.tx)
            .await?;
        self.tx.commit().await?;
        Ok(())
    }
}
pub(super) async fn storage_current_key(
    pool: &PgPool,
    name: &str,
    key: Option<&str>,
) -> anyhow::Result<Uuid> {
    let mut tx = pool.begin().await?;
    lock_mode(&mut tx).await?;
    let row = load_row(&mut tx, name).await?;
    let key_id = if let Some(key) = key {
        let parsed = key.parse::<Uuid>().ok();
        queries::current_storage_key(parsed, key.trim())
            .fetch_optional(&mut *tx)
            .await?
            .context("active block key was not found")
            .map(Some)?
    } else {
        None
    };
    if key_id.is_some() {
        queries::storage_profile_lock(row.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(key_id)?;
    }
    queries::update_storage_key(row.id, key_id)
        .execute(&mut *tx)
        .await?;
    queries::bump_storage_profile(row.id)
        .execute(&mut *tx)
        .await?;
    queries::notify_storage_profile(row.id.to_string())
        .execute(&mut *tx)
        .await?;
    checkpoint_event(
        &mut tx,
        "storage.current_key.update",
        name,
        serde_json::json!({"block_key_id":key_id}),
    )
    .await?;
    tx.commit().await?;
    Ok(row.id)
}

pub(super) async fn set_deployment_mode(
    pool: &PgPool,
    principal: &Principal,
    mode: &str,
) -> Result<()> {
    ensure!(
        matches!(mode, "single" | "cluster"),
        "mode must be single or cluster"
    );
    let mut tx = pool.begin().await?;
    let current: String = queries::deployment_mode_lock().fetch_one(&mut *tx).await?;
    if current == mode {
        tx.rollback().await?;
        return Ok(());
    }
    let (maintenance, tasks, uploads, sources) =
        queries::mode_transition_state().fetch_one(&mut *tx).await?;
    ensure!(
        maintenance,
        "enable maintenance before changing deployment mode"
    );
    ensure!(
        !tasks,
        "finish queued, running and paused tasks before changing deployment mode"
    );
    ensure!(
        !uploads,
        "finish or abort uploads and wait for reads to drain before changing deployment mode"
    );
    ensure!(
        !sources,
        "flush pending uploads and referenced fragments before changing deployment mode"
    );
    if mode == "cluster" {
        let local: bool = queries::cluster_local_storages()
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !local,
            "local storages exist; migrate them to S3 and update their storage before enabling cluster mode"
        );
        let staging: Option<uuid::Uuid> = queries::cluster_staging_id().fetch_one(&mut *tx).await?;
        let valid_staging: bool = queries::cluster_staging_valid(staging)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            valid_staging,
            "cluster mode requires an enabled S3 uploads.staging_storage_id"
        );
    } else {
        let local_node = match principal {
            Principal::Node { id, role } if role == "control" => Some(*id),
            _ => None,
        };
        let other_nodes: bool = queries::cluster_other_nodes(local_node)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            !other_nodes,
            "disable all registered cluster nodes before returning to single mode"
        );
    }
    if mode == "single"
        && let Principal::Node { id, role } = principal
        && role == "control"
    {
        queries::release_node_id(id).execute(&mut *tx).await?;
        queries::release_node_tasks(id).execute(&mut *tx).await?;
        queries::release_node_permits(id).execute(&mut *tx).await?;
    }
    queries::preserve_failed_task_mode(&current)
        .execute(&mut *tx)
        .await?;
    queries::update_deployment_mode(&mode)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn release_node(pool: &PgPool, name: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let id: Uuid = queries::release_node_name(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("node not found or already released"))?;
    queries::release_node_tasks(&id).execute(&mut *tx).await?;
    queries::release_raw_uploads(id).execute(&mut *tx).await?;
    queries::release_node_permits(&id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn heartbeat(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    validate_node(&mut *tx, state, role, key).await?;
    queries::node_seen(state.node_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn begin_report(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    boot: Uuid,
    started: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    validate_node(&mut tx, state, role, key).await?;
    queries::begin_node_report(state.node_id, boot, started)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn report_node(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    report: &crate::model::node::NodeReport,
) -> Result<bool> {
    report.validate()?;
    let mut tx = pool.begin().await?;
    validate_node(&mut tx, state, role, key).await?;
    let written = queries::save_node_report(state.node_id, report)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(written)
}

pub(super) async fn recover_node(pool: &PgPool, name: &str, role: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let registered: Option<(Uuid, String)> = queries::recover_node_lock(&name)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some((id, registered_role)) = registered {
        ensure!(
            registered_role == role,
            "registered node role does not match"
        );
        queries::release_node_id(&id).execute(&mut *tx).await?;
        queries::release_node_tasks(&id).execute(&mut *tx).await?;
        queries::release_raw_uploads(id).execute(&mut *tx).await?;
        queries::release_node_permits(&id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

struct Leadership {
    transaction: Transaction<'static, Postgres>,
}
#[async_trait::async_trait]
impl crate::model::cluster::Leader for Leadership {
    async fn finish(self: Box<Self>) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
}
pub(super) async fn cluster_leader(
    pool: &PgPool,
    key: i64,
) -> Result<Option<Box<dyn crate::model::cluster::Leader>>> {
    let mut transaction = pool.begin().await?;
    let locked: bool = queries::try_cluster_leader(key)
        .fetch_one(&mut *transaction)
        .await?;
    if locked {
        Ok(Some(
            Box::new(Leadership { transaction }) as Box<dyn crate::model::cluster::Leader>
        ))
    } else {
        transaction.rollback().await?;
        Ok(None)
    }
}

impl Principal {
    pub(super) async fn lock_identity(&self, tx: &mut Transaction<'_, Postgres>) -> Result<bool> {
        lock_identity(tx, self).await
    }
    pub(super) async fn lock_admin(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        if lock_identity(tx, self).await? {
            Ok(())
        } else {
            Err(s3s::s3_error!(AccessDenied).into())
        }
    }
}

impl Permit {
    pub(super) async fn lock(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        lock_permit(tx, self).await
    }
}

async fn lock_execution(tx: &mut Transaction<'_, Postgres>, task: Execution) -> Result<()> {
    ensure!(
        queries::lock_task_execution(task.id, task.token)
            .fetch_optional(&mut **tx)
            .await?
            .is_some(),
        "task execution expired"
    );
    Ok(())
}

pub(super) async fn change_task_state(
    pool: &PgPool,
    id: Uuid,
    parent: bool,
    resume: bool,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if parent {
        // Finish locks a child before its parent; controls use the same order.
        queries::lock_task_family(id).fetch_all(&mut *tx).await?;
    }
    if resume {
        queries::refresh_task_profiles(id, parent)
            .execute(&mut *tx)
            .await?;
    }
    let changed = queries::change_task_state(id, parent, resume)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 0 {
        return Ok(false);
    }
    if parent && resume {
        queries::update_parent_progress(id)
            .execute(&mut *tx)
            .await?;
    }
    queries::notify_task(id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn finish_task(pool: &PgPool, input: crate::model::TaskFinish) -> Result<()> {
    let mut tx = pool.begin().await?;
    let task = input.execution;
    let Some((task_type, state, parent)) = queries::lock_task_finish(task)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(());
    };
    let failed = state == "running" && input.error.is_some();
    if failed && matches!(task_type.as_str(), "pack" | "unpack") {
        queries::defer_pack_changes(task.id)
            .execute(&mut *tx)
            .await?;
        queries::defer_pack_maintenance(task.id)
            .execute(&mut *tx)
            .await?;
    }
    if failed && matches!(task_type.as_str(), "upload" | "cache_flush") {
        queries::backoff_pending_uploads(task.id, input.error.as_deref().unwrap())
            .execute(&mut *tx)
            .await?;
    }
    if (failed || state != "running")
        && matches!(
            task_type.as_str(),
            "pack" | "unpack" | "rewrite" | "upload" | "cache_flush"
        )
    {
        release_task_inputs(&mut tx, task.id).await?;
    }
    if failed {
        queries::fail_owned_task(task.id, input.error.as_deref().unwrap(), task.token)
            .execute(&mut *tx)
            .await?;
    } else if state == "running" {
        queries::requeue_owned_task(task.id, task.token)
            .execute(&mut *tx)
            .await?;
    } else {
        queries::release_task_lease(task.id, task.token)
            .execute(&mut *tx)
            .await?;
        if state == "completed"
            && let Some(parent) = parent
        {
            queries::update_parent_progress(parent)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn sample_node_runtime(
    pool: &PgPool,
    state: &crate::cluster::NodeState,
    role: &str,
    key: &crate::content::codec::Key,
    sample: &crate::model::metrics::NodeSample,
) -> Result<bool> {
    sample.validate()?;
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    validate_node(&mut tx, state, role, key).await?;
    let changed = queries::node_runtime_sample(state.node_id, sample)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}
pub(super) async fn aggregate_runtime(pool: &PgPool, interval: u64, retention: u64) -> Result<()> {
    use crate::model::metrics::{NodeRuntimePoint, aggregate_runtime};
    let mut tx = pool.begin().await?;
    queries::runtime_diagnostic_limits()
        .execute(&mut *tx)
        .await?;
    if !queries::runtime_aggregate_lock()
        .fetch_one(&mut *tx)
        .await?
    {
        return Ok(());
    }
    let (at, expected, due) = queries::runtime_aggregate_due(interval as f64)
        .fetch_one(&mut *tx)
        .await?;
    if due {
        let rows =
            queries::runtime_node_window(interval.saturating_mul(3).saturating_add(30) as f64)
                .fetch_all(&mut *tx)
                .await?;
        let expected = rows.first().map(|row| row.3).unwrap_or(expected);
        let samples = rows
            .into_iter()
            .filter_map(|(id, at, sample, _)| {
                Some(NodeRuntimePoint {
                    node_id: id?,
                    received_at: at?,
                    sample: sample?.0,
                })
            })
            .collect::<Vec<_>>();
        let point = aggregate_runtime(at, expected, interval, &samples)?;
        queries::cluster_runtime_sample(&point)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if due {
        // Commit each bounded cleanup batch so a deadline cannot undo useful progress.
        for _ in 0..8 {
            let mut tx = pool.begin().await?;
            queries::runtime_diagnostic_limits()
                .execute(&mut *tx)
                .await?;
            let removed = queries::node_runtime_expire(retention as f64)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            queries::cluster_runtime_expire(retention as f64)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            if removed < 1000 {
                break;
            }
        }
    }
    Ok(())
}

pub(super) async fn require_sources(
    db: &mut sqlx::PgConnection,
    stream: Option<Id>,
    chunks: &[Id],
) -> Result<()> {
    use futures_util::TryStreamExt;
    // Mapping changes take FOR UPDATE; references may update liveness under KEY SHARE.
    // Lock logical rows before physical rows, whose SHARE locks block quarantine.
    let mut packed = stream.is_some();
    for phase in 0..3 {
        if phase == 2 && !packed {
            break;
        }
        let mut query = queries::source_locks(stream, chunks, phase);
        let mut rows = query.build_query_scalar::<bool>().fetch(&mut *db);
        while let Some(has_pack) = rows.try_next().await? {
            if phase == 0 {
                packed |= has_pack;
            }
        }
    }
    ensure!(
        !queries::unavailable_sources(stream, chunks)
            .build_query_scalar::<bool>()
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

async fn require_writing_stream(db: &mut PgConnection, stream: Id) -> Result<()> {
    ensure!(
        queries::source_writing_stream(stream)
            .fetch_optional(db)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn read_source_snapshot(
    pool: &sqlx::PgPool,
    scope: &crate::model::source::SourceScope,
) -> Result<crate::model::source::SourceSnapshot> {
    let mut tx = pool.begin().await?;
    let snapshot = queries::source_snapshot(&mut tx, scope, false).await?;
    tx.commit().await?;
    Ok(snapshot)
}

pub(super) async fn source_update(
    pool: &sqlx::PgPool,
    update: crate::model::source::SourceUpdate<'_>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let operation = update.operation;
    operation.principal.lock_admin(&mut tx).await?;
    ensure!(
        queries::source_mode(&operation.mode)
            .fetch_one(&mut *tx)
            .await?,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::source_executor(update.execution.id)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(operation.mode == "single"),
        crate::model::Error::OperationAborted
    );
    lock_execution(&mut tx, update.execution).await?;
    let snapshot = queries::source_snapshot(&mut tx, &operation.snapshot.scope, true).await?;
    ensure!(
        snapshot.state == "ready" && snapshot.fingerprint()? == operation.snapshot.fingerprint()?,
        crate::model::Error::OperationAborted
    );
    ensure!(
        snapshot.quarantine_code.is_none() == operation.code.is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        operation
            .code
            .as_deref()
            .is_none_or(crate::model::source::quarantine_code),
        s3s::s3_error!(InvalidArgument)
    );
    if update.apply {
        ensure!(
            queries::change_source(&snapshot, operation.code.as_deref())?
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let action = if operation.code.is_some() {
            "source.quarantine"
        } else {
            "source.recheck"
        };
        let detail = serde_json::json!({"source":snapshot.summary()?,"revision":(snapshot.revision+1).to_string(),"code":operation.code,"requested_by":operation.principal.user_id(),"token_id":operation.principal.token_id(),"task_id":update.execution.id});
        queries::source_result(update.execution.id, serde_json::json!({"result":detail}))
            .execute(&mut *tx)
            .await?;
        queries::complete_task(update.execution.id)
            .execute(&mut *tx)
            .await?;
        queries::source_audit(
            &operation.principal,
            action,
            &snapshot.location().to_string(),
            detail,
        )?
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn require_inputs(db: &mut sqlx::PgConnection, chunks: &[Id]) -> Result<()> {
    require_sources(db, None, chunks).await?;
    ensure!(
        !queries::quarantined_inputs(chunks)
            .build_query_scalar::<bool>()
            .fetch_one(db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    Ok(())
}

async fn lock_splice_sources(db: &mut sqlx::PgConnection, streams: &[Id], chunk: Id) -> Result<()> {
    use futures_util::TryStreamExt;
    for physical in [false, true] {
        let mut query = queries::splice_locks(streams, chunk, physical);
        let mut rows = query.build().fetch(&mut *db);
        while rows.try_next().await?.is_some() {}
    }
    require_sources(db, None, &[chunk]).await
}
async fn source_input_stamp(db: &mut sqlx::PgConnection, chunks: &[Id]) -> Result<Value> {
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(chunks)
            .build_query_as()
            .fetch_all(db)
            .await?;
    Ok(Value::Object(
        rows.into_iter()
            .map(|(id, encoding, revision, pack, pack_revision)| {
                (
                    id.to_string(),
                    json!([encoding, revision, pack, pack_revision]),
                )
            })
            .collect(),
    ))
}
async fn record_source_inputs(
    db: &mut sqlx::PgConnection,
    task: Uuid,
    chunks: &[Id],
) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    queries::save_source_inputs(task, &stamp)
        .execute(db)
        .await?;
    Ok(())
}
async fn check_source_inputs(db: &mut sqlx::PgConnection, task: Uuid, chunks: &[Id]) -> Result<()> {
    require_inputs(db, chunks).await?;
    let stamp = source_input_stamp(db, chunks).await?;
    let saved = queries::saved_source_inputs(task).fetch_one(db).await?;
    ensure!(
        stamp
            .as_object()
            .unwrap()
            .iter()
            .all(|(id, value)| saved.get(id) == Some(value)),
        crate::model::Error::OperationAborted
    );
    Ok(())
}

async fn repair_writer(
    db: &mut sqlx::PgConnection,
    stream: Id,
    node: Option<&crate::model::recovery::RepairNode>,
) -> Result<()> {
    let authority = queries::write_authorization(stream)
        .fetch_one(&mut *db)
        .await?
        .0;
    lock_permit(&mut *db, &authority).await?;
    if let Some(node) = node {
        validate_node(&mut *db, &node.state, &node.role, &node.key).await?;
        ensure!(
            queries::repair_cluster_mode().fetch_one(&mut *db).await?,
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::repair_node(node.state.node_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    } else {
        ensure!(
            queries::repair_mode().fetch_one(&mut *db).await?,
            crate::model::Error::OperationAborted
        );
    }
    let observed = queries::repair_owner(stream)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    let (bucket, stream_type, owner, token) = &observed;
    if let Some(owner) = *owner {
        let token = token.ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            queries::lock_owned_task(&owner, &token)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::lock_bucket_uploads(*bucket)
            .fetch_one(&mut *db)
            .await?
            .0
            == "active",
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "part" {
        // Upload ownership precedes stream locks, as in multipart publication.
        ensure!(
            queries::repair_upload(stream)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    if owner.is_none() && stream_type == "part" {
        ensure!(
            queries::repair_part(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    ensure!(
        queries::repair_stream(stream)
            .fetch_optional(&mut *db)
            .await?
            == Some(observed.clone()),
        crate::model::Error::OperationAborted
    );
    if owner.is_none() && stream_type == "object" {
        ensure!(
            queries::repair_object(stream)
                .fetch_optional(&mut *db)
                .await?
                == Some(stream),
            crate::model::Error::OperationAborted
        );
    }
    if node.is_some() {
        ensure!(
            queries::repair_lease(stream)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

async fn repair_sources(
    db: &mut sqlx::PgConnection,
    chunk: &Chunk,
) -> Result<Vec<crate::model::source::SourceSnapshot>> {
    use crate::model::source::{SourceScope, SourceType};
    ensure!(
        !queries::repair_pending(chunk.id)
            .fetch_one(&mut *db)
            .await?,
        crate::model::Error::SourceUnavailable
    );
    let rows: Vec<(Id, Option<Id>, Option<i64>, Option<Id>, Option<i64>)> =
        queries::source_input_versions(&[chunk.id])
            .build_query_as()
            .fetch_all(&mut *db)
            .await?;
    ensure!(rows.len() == 1, crate::model::Error::OperationAborted);
    let mut sources = Vec::with_capacity(2);
    for (source, id) in [
        (SourceType::Chunk, rows[0].1),
        (SourceType::Pack, rows[0].3),
    ] {
        if let Some(id) = id {
            let scope = SourceScope {
                storage_id: chunk
                    .storage_id
                    .ok_or(crate::model::Error::OperationAborted)?,
                source,
                id: id.to_string(),
            };
            let snapshot = queries::source_snapshot(db, &scope, true).await?;
            ensure!(
                // Only lost RAM placeholders are included before encoding is ready.
                snapshot.repairable(),
                crate::model::Error::SourceUnavailable
            );
            sources.push(snapshot);
        }
    }
    ensure!(!sources.is_empty(), crate::model::Error::SourceUnavailable);
    Ok(sources)
}

pub(super) async fn prepare_repair(
    pool: &sqlx::PgPool,
    input: ChunkCreate,
    node: Option<crate::model::recovery::RepairNode>,
) -> Result<crate::model::recovery::RepairAllocation> {
    use crate::model::recovery::{Repair, RepairAllocation};
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, input.stream, node.as_ref()).await?;
    let mut chunk = queries::repair_chunk(&input)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    match require_sources(&mut tx, None, &[chunk.id]).await {
        Ok(()) => {
            tx.commit().await?;
            return Ok(RepairAllocation::Existing(chunk));
        }
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) => {}
        Err(error) => return Err(error),
    }
    let sources = repair_sources(&mut tx, &chunk).await?;
    let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        sources
            .iter()
            .all(|source| source.storage_revision == revision),
        crate::model::Error::OperationAborted
    );
    chunk.location_id = Uuid::nil();
    chunk.algorithm = input.algorithm;
    chunk.block_key_id = input.key;
    chunk.stored_size = None;
    chunk.compressed = false;
    chunk.nonce = None;
    chunk.encoding_id = queries::insert_repair(&chunk, input.stream)
        .fetch_one(&mut *tx)
        .await?;
    if let Id::Uuid(id) = chunk.encoding_id {
        chunk.location_id = id;
    }
    let repair = Repair {
        stream: input.stream,
        node,
        chunk,
        sources,
    };
    repair.validate()?;
    tx.commit().await?;
    Ok(RepairAllocation::Candidate(Box::new(repair)))
}

pub(super) async fn repair_metadata(
    pool: &sqlx::PgPool,
    repair: &crate::model::recovery::Repair,
) -> Result<()> {
    repair.validate()?;
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, false)?;
    ensure!(
        queries::save_repair_metadata(&repair.chunk, repair.stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );

    tx.commit().await?;
    Ok(())
}

pub(super) async fn finish_repair(
    pool: &sqlx::PgPool,
    repair: &crate::model::recovery::Repair,
    offset: i64,
) -> Result<Chunk> {
    repair.validate()?;
    let mut tx = pool.begin().await?;
    repair_writer(&mut tx, repair.stream, repair.node.as_ref()).await?;
    let input = ChunkCreate {
        stream: repair.stream,
        storage: repair
            .chunk
            .storage_id
            .ok_or(crate::model::Error::OperationAborted)?,
        hash: repair.chunk.hash.as_slice().try_into()?,
        raw_size: repair.chunk.raw_size,
        algorithm: repair.chunk.algorithm.clone(),
        key: repair.chunk.block_key_id,
    };
    let current = queries::repair_chunk(&input)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        current.id == repair.chunk.id,
        crate::model::Error::OperationAborted
    );
    let healthy = match require_sources(&mut tx, None, &[current.id]).await {
        Ok(()) => true,
        Err(error)
            if matches!(
                crate::model::error(&error),
                Some(crate::model::Error::SourceUnavailable)
            ) =>
        {
            false
        }
        Err(error) => return Err(error),
    };
    let sources = if healthy {
        Vec::new()
    } else {
        repair_sources(&mut tx, &current).await?
    };
    let candidate = queries::repair_candidate(repair.chunk.encoding_id, repair.stream)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    repair.check_candidate(&candidate, true)?;
    let result = if healthy {
        queries::discard_verified_repair(candidate.encoding_id, repair.stream)
            .execute(&mut *tx)
            .await?;
        current
    } else {
        ensure!(
            sources.len() == repair.sources.len(),
            crate::model::Error::OperationAborted
        );
        for (current, expected) in sources.iter().zip(&repair.sources) {
            ensure!(
                current.fingerprint()? == expected.fingerprint()?,
                crate::model::Error::OperationAborted
            );
        }
        let revision = queries::repair_target(input.storage, input.key, &input.algorithm)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            sources
                .iter()
                .all(|source| source.storage_revision == revision),
            crate::model::Error::OperationAborted
        );
        queries::retire_repair_sources(current.id)
            .execute(&mut *tx)
            .await?;
        ensure!(
            queries::publish_repair(candidate.encoding_id, repair.stream)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::publish_repaired_chunk(&candidate)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        let authority = queries::write_authorization(repair.stream)
            .fetch_one(&mut *tx)
            .await?
            .0;
        queries::source_audit(&authority.principal, "source.recover", &current.id.to_string(), serde_json::json!({"stream_id":repair.stream,"storage_id":input.storage,"chunk_id":current.id.to_string(),"encoding_id":candidate.encoding_id.to_string(),"sources":sources.iter().map(|source| source.summary()).collect::<Result<Vec<_>>>()?}))?.execute(&mut *tx).await?;
        candidate
    };
    queries::insert_chunk_extent(repair.stream, offset, result.raw_size, result.id)
        .execute(&mut *tx)
        .await?;
    queries::reference_chunk(result.id)
        .execute(&mut *tx)
        .await?;
    queries::pin_chunk(result.id, repair.stream)
        .execute(&mut *tx)
        .await?;
    queries::attach_pending_upload(result.id, repair.stream, offset)
        .execute(&mut *tx)
        .await?;
    queries::resize_writing_stream(repair.stream, offset + i64::from(result.raw_size))
        .execute(&mut *tx)
        .await?;
    if repair.node.is_some() {
        ensure!(
            queries::repair_lease(repair.stream)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    tx.commit().await?;
    Ok(result)
}

pub(super) async fn begin_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    storage: Uuid,
    revision: i64,
    encoding: Option<&crate::model::cache::EncodingPolicy>,
) -> Result<(Id, Uuid)> {
    let mut tx = pool.begin().await?;
    let (storage_type, unencoded, current, cdc, background) = queries::raw_target(stream, storage)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    ensure!(
        matches!(storage_type.as_str(), "local" | "s3")
            && current == revision
            && if encoding.is_some() {
                cdc && background
            } else {
                unencoded && !cdc
            },
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_prepare(stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    let id = queries::raw_insert(
        stream,
        storage,
        revision,
        if storage_type == "local" {
            "local"
        } else {
            "shared_s3"
        },
    )
    .fetch_one(&mut *tx)
    .await?;
    if let Some(encoding) = encoding {
        encoding.validate()?;
        ensure!(
            queries::conversion_key(storage, encoding.block_key_id)
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
        if let Some(key) = encoding.block_key_id {
            let (_, algorithm) = queries::replacement_key(key)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(crate::model::Error::OperationAborted)?;
            ensure!(
                algorithm == encoding.algorithm,
                crate::model::Error::OperationAborted
            );
        }
        let (bucket, object_key, payload_revision) = queries::conversion_stream(stream)
            .fetch_one(&mut *tx)
            .await?;
        let job = crate::model::raw::Conversion {
            stream,
            source: id.into(),
            bucket,
            object_key,
            payload_revision,
            encoding: encoding.clone(),
            target_key_id: encoding.block_key_id,
        };
        let mut detail = serde_json::to_value(job)?;
        detail["operation"] = serde_json::json!("cdc");
        let task = Uuid::new_v4();
        queries::insert_task(task, "upload", Some(bucket), detail, None)
            .execute(&mut *tx)
            .await?;
        queries::conversion_link(id.into(), task)
            .execute(&mut *tx)
            .await?;
    } else if background {
        queries::raw_enqueue_processing(stream, id.into())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((id.into(), id))
}

pub(super) async fn finish_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    size: i64,
    digest: &[u8],
    cached: Option<(i64, bool, bool, Option<Uuid>)>,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    ensure!(
        size >= 0 && digest.len() == 16,
        "invalid received raw source"
    );
    let mut tx = pool.begin().await?;
    if cached.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_finish(stream, source, size, digest, cached.is_none())
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::raw_attach(stream, source, size)
        .execute(&mut *tx)
        .await?;
    if let Some((revision, deduplicate, background, boot)) = cached {
        ensure!(
            queries::raw_stage(source, revision, boot)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
        queries::raw_optional_delete(source)
            .execute(&mut *tx)
            .await?;
        queries::raw_queue(source, deduplicate, background, node.map(|n| n.0.node_id))
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn validate_raw(db: &mut sqlx::PgConnection, stream: Id, size: i64) -> Result<()> {
    let sources = queries::raw_sources_lock(stream)
        .fetch_all(&mut *db)
        .await?;
    ensure!(!sources.is_empty(), "raw generation has no source");
    for source in sources {
        source.validate()?;
    }
    let (end, contiguous) = queries::raw_continuity(stream).fetch_one(db).await?;
    ensure!(
        end == size && contiguous,
        "cannot publish an incomplete raw generation"
    );
    Ok(())
}

pub(super) async fn deduplicate_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    hash: &[u8],
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let received = queries::raw_source(source).fetch_one(pool).await?;
    let mut tx = pool.begin().await?;
    queries::claim_chunk_hash(received.storage_id, hash)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let current = queries::raw_source(source).fetch_one(&mut *tx).await?;
    ensure!(current == received, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, stream, current.size).await?;
    let reused = share_raw(&mut tx, stream, &current, hash).await?;
    tx.commit().await?;
    Ok(reused)
}

async fn share_raw(
    db: &mut sqlx::PgConnection,
    stream: Id,
    current: &crate::model::raw::RawSource,
    hash: &[u8],
) -> Result<bool> {
    let duplicate = queries::raw_duplicate(current.storage_id, hash, current.size)
        .fetch_optional(&mut *db)
        .await?;
    let mut reused = false;
    if let Some(candidate) = duplicate {
        if candidate.id == current.id {
            return Ok(false);
        }
        if candidate.validate().is_ok()
            && !candidate.pending_upload
            && !current.pending_upload
            && candidate.medium == current.medium
        {
            ensure!(
                queries::raw_share(stream, current.id, candidate.id, current.size)
                    .execute(&mut *db)
                    .await?
                    .rows_affected()
                    == 1,
                crate::model::Error::OperationAborted
            );
            queries::raw_reference(candidate.id)
                .execute(&mut *db)
                .await?;
            queries::raw_touch_unreferenced(current.id)
                .execute(&mut *db)
                .await?;
            reused = true;
        } else {
            queries::raw_hash(candidate.id, None)
                .execute(&mut *db)
                .await?;
        }
    }
    if !reused {
        queries::raw_hash(current.id, Some(hash))
            .execute(&mut *db)
            .await?;
    }
    Ok(reused)
}

pub(super) async fn multipart_file(
    pool: &PgPool,
    upload: Uuid,
    node: Option<Uuid>,
) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("active"),
        crate::model::Error::OperationAborted
    );
    let id = if let Some(id) = queries::current_multipart_file(upload, node)
        .fetch_optional(&mut *tx)
        .await?
    {
        id
    } else {
        queries::insert_multipart_file(upload, node)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?
    };
    tx.commit().await?;
    Ok(id)
}

pub(super) async fn remap_multipart_file(
    pool: &PgPool,
    upload: Uuid,
    source: Id,
    ranges: &[crate::model::multipart::FileRemap],
) -> Result<()> {
    ensure!(
        !ranges.is_empty() && ranges.len() <= 64,
        "invalid multipart checkpoint batch"
    );
    let mut tx = pool.begin().await?;
    ensure!(
        queries::lock_local_upload(upload)
            .fetch_optional(&mut *tx)
            .await?
            .as_deref()
            == Some("completing"),
        crate::model::Error::OperationAborted
    );
    queries::insert_multipart_checkpoint(upload, source)
        .execute(&mut *tx)
        .await?;
    for range in ranges {
        ensure!(
            queries::remap_multipart_range(upload, source, range)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let files: std::collections::BTreeSet<_> = ranges.iter().map(|range| range.file).collect();
    for file in files {
        queries::pending_multipart_reclaim(file)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn claim_raw_processing(
    pool: &sqlx::PgPool,
    node: crate::model::raw::Processor<'_>,
    token: Uuid,
) -> Result<Option<crate::model::raw::RawProcessing>> {
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    let single = node.is_none();
    let Some(stream) = queries::raw_processing_due(single)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_processing_claim(stream, token, single)
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_processing_record(stream)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(super) async fn finish_raw_processing(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawProcessing,
    source: &crate::model::raw::RawSource,
    hash: &[u8],
    lease: Option<Uuid>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(hash.len() == 32, "invalid raw content hash");
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    queries::claim_chunk_hash(source.storage_id, hash)
        .execute(&mut *tx)
        .await?;
    let current_stream = queries::raw_processing_stream(work)
        .fetch_optional(&mut *tx)
        .await?;
    ensure!(
        queries::raw_processing_owned(work)
            .fetch_optional(&mut *tx)
            .await?
            == Some(source.id),
        crate::model::Error::OperationAborted
    );
    if current_stream.is_none() {
        queries::raw_processing_delete(work)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(true);
    }
    if let Some(lease) = lease {
        if queries::raw_processing_readers(work.stream, lease)
            .fetch_one(&mut *tx)
            .await?
        {
            return Ok(false);
        }
    }
    let current = queries::raw_source(source.id).fetch_one(&mut *tx).await?;
    ensure!(current == *source, crate::model::Error::SourceUnavailable);
    current.validate()?;
    validate_raw(&mut tx, work.stream, current.size).await?;
    if share_raw(&mut tx, work.stream, &current, hash).await? {
        queries::raw_processing_changed(work.stream)
            .execute(&mut *tx)
            .await?;
    }
    queries::raw_processing_delete(work)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

async fn raw_processor(
    db: &mut PgConnection,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    if let Some((state, role, key)) = node {
        validate_node(&mut *db, state, role, key).await?;
        ensure!(
            queries::repair_node(state.node_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
        ensure!(
            queries::repair_cluster_mode().fetch_one(db).await?,
            crate::model::Error::OperationAborted
        );
    } else {
        ensure!(
            queries::repair_mode().fetch_one(db).await?,
            crate::model::Error::OperationAborted
        );
    }
    Ok(())
}

pub(super) async fn renew_raw_processing(
    pool: &PgPool,
    work: &crate::model::raw::RawProcessing,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_processor(&mut tx, node).await?;
    let renewed = queries::renew_raw_processing(work)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(renewed)
}

pub(super) async fn reserve_nonce(
    pool: &PgPool,
    key: Uuid,
    type_tag: u8,
    units: u32,
) -> Result<(i64, i64)> {
    ensure!(matches!(type_tag, 0 | 1), "invalid nonce type");
    let mut tx = pool.begin().await?;
    let previous = queries::nonce_counter(key, type_tag)
        .fetch_optional(&mut *tx)
        .await?
        .context("active encryption key nonce counter is missing")?;
    let range = crate::model::key::nonce_reservation(previous, units)?;
    ensure!(
        queries::advance_nonce(key, type_tag, previous, range.1)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "nonce reservation changed"
    );
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("nonce-after-commit").await;
    Ok(range)
}

use crate::model::key_replacement::{
    self, KeyReplacementInput, KeyReplacementPreview, KeyReplacementResult, ReplacementRow,
};

pub(super) async fn key_replacement_preview(
    pool: &PgPool,
    principal: &Principal,
    source: Uuid,
    input: KeyReplacementInput,
) -> Result<KeyReplacementPreview> {
    key_replacement::validate(&input, source)?;
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    let storages = queries::replacement_storages(source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        !storages.is_empty(),
        s3s::s3_error!(InvalidRequest, "key has no associated storages")
    );
    ensure!(
        serde_json::to_vec(&storages)?.len() <= 131072,
        s3s::s3_error!(InvalidRequest, "associated storage list is too large")
    );
    let (source_name, mut algorithm) = queries::replacement_key(source)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    let id = Uuid::new_v4();
    let target = input.target.unwrap_or_else(Uuid::new_v4);
    let target_name = if input.target.is_some() {
        let (name, selected_algorithm) = queries::replacement_key(target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "target must be an active block key"))?;
        ensure!(
            queries::replacement_capacity(target)
                .fetch_one(&mut *tx)
                .await?,
            s3s::s3_error!(InvalidRequest, "target key cannot reserve more nonce IDs")
        );
        algorithm = selected_algorithm;
        name
    } else {
        input
            .name
            .unwrap_or_else(|| format!("key-{}", target.simple()))
            .trim()
            .to_owned()
    };
    for storage in &storages {
        queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?
            .validate_key(Some(target))?;
    }
    let preview = KeyReplacementPreview {
        id,
        source,
        source_name,
        target,
        target_name,
        algorithm,
        create: input.target.is_none(),
        expires_at: Utc::now() + chrono::Duration::minutes(10),
        storages,
    };
    queries::insert_key_replacement(&actor, &preview)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(preview)
}

pub(super) async fn key_replacement(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
) -> Result<ReplacementRow> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    let row = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    tx.commit().await?;
    Ok(row)
}

pub(super) async fn replace_keys(
    pool: &PgPool,
    principal: &Principal,
    id: Uuid,
    input: Option<ManagedKeyCreate>,
) -> Result<KeyReplacementResult> {
    let actor = key_replacement::actor(principal)?;
    let mut tx = admin_transaction(pool, principal).await?;
    lock_mode(&mut tx).await?;
    let (preview, result) = queries::key_replacement(id, &actor)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
    if let Some(result) = result {
        tx.commit().await?;
        return Ok(result.0);
    }
    let preview = preview.0;
    ensure!(
        preview.expires_at > Utc::now(),
        crate::model::Error::OperationAborted
    );
    let storages = queries::replacement_storages(preview.source)
        .fetch_all(&mut *tx)
        .await?;
    ensure!(
        storages == preview.storages,
        crate::model::Error::StorageProfileConflict
    );
    for storage in &storages {
        let profile = queries::storage_profile_lock(storage.id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            profile.revision == storage.revision,
            crate::model::Error::StorageProfileConflict
        );
        profile.validate_key(Some(preview.target))?;
    }
    ensure!(
        queries::replacement_key(preview.source)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if preview.create {
        let input = input.context("new key material is required")?;
        ensure!(
            input.id == preview.target
                && input.name == preview.target_name
                && input.algorithm == preview.algorithm
                && input.purpose == "block",
            s3s::s3_error!(InvalidArgument)
        );
        queries::insert_managed_key(&input)
            .fetch_one(&mut *tx)
            .await?;
        queries::initialize_nonce_counters(input.id)
            .execute(&mut *tx)
            .await?;
    } else {
        let target = queries::replacement_key(preview.target)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(crate::model::Error::OperationAborted)?;
        ensure!(
            target == (preview.target_name.clone(), preview.algorithm.clone())
                && queries::replacement_capacity(preview.target)
                    .fetch_one(&mut *tx)
                    .await?,
            crate::model::Error::OperationAborted
        );
    }
    for storage in &storages {
        queries::update_storage_key(storage.id, Some(preview.target))
            .execute(&mut *tx)
            .await?;
        queries::bump_storage_profile(storage.id)
            .execute(&mut *tx)
            .await?;
        queries::notify_storage_profile(storage.id.to_string())
            .execute(&mut *tx)
            .await?;
    }
    let result = KeyReplacementResult {
        preview,
        committed_at: Utc::now(),
    };
    queries::complete_key_replacement(&result)
        .execute(&mut *tx)
        .await?;
    checkpoint_event(&mut tx, "managed_key.replace", &id.to_string(), serde_json::json!({"source":result.preview.source,"target":result.preview.target,"created":result.preview.create,"storages":result.preview.storages})).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-before-commit").await;
    tx.commit().await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("key-replacement-after-commit").await;
    Ok(result)
}

pub(super) async fn reuse_raw(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
    storage: Uuid,
    size: i64,
    digest: &[u8],
    hash: &[u8],
) -> Result<bool> {
    ensure!(
        size >= 0 && digest.len() == 16 && hash.len() == 32,
        "invalid received raw identity"
    );
    if queries::raw_reusable(storage, hash, size, digest)
        .fetch_optional(pool)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    #[cfg(feature = "fault-injection")]
    crate::faults::point("raw-candidate-found").await;
    let mut tx = pool.begin().await?;
    queries::claim_chunk_hash(storage, hash)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_lock_stream(stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_receiving(source, stream)
            .fetch_optional(&mut *tx)
            .await?
            == Some(storage),
        crate::model::Error::OperationAborted
    );
    let candidate = queries::raw_duplicate(storage, hash, size)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(candidate) = candidate.filter(|candidate| {
        candidate.validate().is_ok()
            && candidate.medium == "shared_s3"
            && candidate.digest.as_deref() == Some(digest)
    }) else {
        tx.commit().await?;
        return Ok(false);
    };
    queries::raw_attach(stream, candidate.id, size)
        .execute(&mut *tx)
        .await?;
    queries::raw_reference(candidate.id)
        .execute(&mut *tx)
        .await?;
    ensure!(
        queries::raw_discard(source, stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn claim_raw_upload(
    pool: &sqlx::PgPool,
    token: Uuid,
    before: Option<chrono::DateTime<chrono::Utc>>,
    node: crate::model::raw::Processor<'_>,
    urgent: &[Uuid],
) -> Result<Option<crate::model::raw::RawUpload>> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let Some(source) = queries::raw_upload_due(before, node.map(|n| n.0.node_id), urgent)
        .build_query_scalar::<Id>()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::raw_upload_claim(source, token, node.map(|n| n.0.node_id))
        .execute(&mut *tx)
        .await?;
    let work = queries::raw_upload_record(source)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(work))
}

pub(super) async fn reuse_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    hash: &[u8],
    streams: &[Id],
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(
        hash.len() == 32 && streams.len() <= 128,
        "invalid raw upload merge"
    );
    if queries::raw_reusable(
        work.source.storage_id,
        hash,
        work.source.size,
        work.source.digest.as_deref().unwrap_or_default(),
    )
    .fetch_optional(pool)
    .await?
    .is_none()
    {
        return Ok(false);
    }
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    queries::claim_chunk_hash(work.source.storage_id, hash)
        .execute(&mut *tx)
        .await?;
    for stream in streams {
        queries::raw_upload_stream_lock(*stream)
            .fetch_optional(&mut *tx)
            .await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node.map(|n| n.0.node_id))
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some() && current.source == work.source,
        crate::model::Error::OperationAborted
    );
    current.source.validate()?;
    let candidate = queries::raw_duplicate(work.source.storage_id, hash, work.source.size)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(candidate) = candidate.filter(|c| {
        c.id != work.source.id
            && c.validate().is_ok()
            && !c.pending_upload
            && c.medium == "shared_s3"
            && c.digest == work.source.digest
    }) else {
        return Ok(false);
    };
    for stream in streams {
        if queries::raw_upload_share(*stream, work.source.id, candidate.id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            > 0
        {
            queries::raw_processing_changed(*stream)
                .execute(&mut *tx)
                .await?;
        }
    }
    queries::raw_reference(candidate.id)
        .execute(&mut *tx)
        .await?;
    queries::raw_touch_unreferenced(work.source.id)
        .execute(&mut *tx)
        .await?;
    if !queries::raw_upload_references(work.source.id)
        .fetch_one(&mut *tx)
        .await?
    {
        queries::raw_upload_delete(work.source.id)
            .execute(&mut *tx)
            .await?;
    } else {
        queries::raw_upload_retry(work, None)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn complete_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    hash: Option<&[u8]>,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    ensure!(
        hash.is_none_or(|h| h.len() == 32),
        "invalid raw upload hash"
    );
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    if let Some(hash) = hash {
        queries::claim_chunk_hash(work.source.storage_id, hash)
            .execute(&mut *tx)
            .await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node.map(|n| n.0.node_id))
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some() && current.source == work.source,
        crate::model::Error::OperationAborted
    );
    current.source.validate()?;
    if current.source.pending_upload {
        ensure!(
            queries::raw_uploaded(work.source.id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let mut duplicate = false;
    if let Some(hash) = hash {
        if let Some(candidate) =
            queries::raw_duplicate(work.source.storage_id, hash, work.source.size)
                .fetch_optional(&mut *tx)
                .await?
        {
            duplicate = candidate.id != work.source.id
                && candidate.validate().is_ok()
                && !candidate.pending_upload
                && candidate.medium == "shared_s3"
                && candidate.digest == work.source.digest;
            if !duplicate && candidate.id != work.source.id {
                queries::raw_hash(candidate.id, None)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        if !duplicate {
            queries::raw_hash(work.source.id, Some(hash))
                .execute(&mut *tx)
                .await?;
        }
    }
    if duplicate {
        queries::raw_upload_hash(work.source.id, hash)
            .execute(&mut *tx)
            .await?;
    } else {
        queries::raw_upload_delete(work.source.id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn abandon_raw_upload(
    pool: &sqlx::PgPool,
    work: &crate::model::raw::RawUpload,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    if node.is_some() {
        raw_upload_processor(&mut tx, node).await?;
    }
    if node.is_some() {
        ensure!(
            queries::raw_upload_owned(work, node.map(|n| n.0.node_id))
                .fetch_one(&mut *tx)
                .await?,
            crate::model::Error::OperationAborted
        );
    }
    let current = queries::raw_upload_record(work.source.id)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        current.token == work.token && current.token.is_some(),
        crate::model::Error::OperationAborted
    );
    if queries::raw_upload_abandon(work.source.id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 0
    {
        return Ok(false);
    }
    queries::raw_upload_delete(work.source.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn renew_raw_upload(
    pool: &PgPool,
    work: &crate::model::raw::RawUpload,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let renewed = queries::renew_raw_upload(work, node.map(|n| n.0.node_id))
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(renewed)
}

async fn raw_upload_processor(
    db: &mut PgConnection,
    node: crate::model::raw::Processor<'_>,
) -> Result<()> {
    if let Some((state, role, key)) = node {
        validate_node(&mut *db, state, role, key).await?;
        ensure!(
            queries::repair_node(state.node_id)
                .fetch_optional(&mut *db)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
    }
    // Cache flush remains available while maintenance blocks ordinary writes.
    ensure!(
        queries::raw_upload_mode(node.is_some())
            .fetch_one(db)
            .await?,
        crate::model::Error::OperationAborted
    );
    Ok(())
}

pub(super) async fn renew_completion(
    pool: &PgPool,
    upload: Uuid,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::renew_completion(upload, token)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}

pub(super) async fn begin_remote(
    pool: &PgPool,
    upload: Uuid,
    storage: Uuid,
    revision: i64,
    node: crate::model::raw::Processor<'_>,
) -> Result<RemoteUpload> {
    let mut tx = pool.begin().await?;
    ensure!(node.is_some(), crate::model::Error::OperationAborted);
    raw_upload_processor(&mut tx, node).await?;
    let (state, source) = queries::remote_upload_lock(upload)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(state == "active", crate::model::Error::OperationAborted);
    if source.is_none() {
        ensure!(
            queries::insert_remote(upload, storage, revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                == 1,
            crate::model::Error::OperationAborted
        );
    }
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(value)
}
pub(super) async fn claim_remote(
    pool: &PgPool,
    upload: Uuid,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let claimed = queries::claim_remote(upload, token)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(claimed)
}
pub(super) async fn initialized_remote(
    pool: &PgPool,
    upload: Uuid,
    token: Uuid,
    upstream: Option<&str>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    ensure!(
        upstream.is_none_or(|id| !id.is_empty() && id.len() <= 4096),
        "invalid remote upload identity"
    );
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::remote_initialized(upload, token, upstream)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(changed)
}
pub(super) async fn claim_remote_part(
    pool: &PgPool,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
    stream: Option<Id>,
) -> Result<Option<RemotePart>> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let node = node.context("native parts require a node")?.0.node_id;
    let Some((upload, number)) = queries::remote_part_due(node, stream)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::remote_part_claim(upload, number, token)
        .execute(&mut *tx)
        .await?;
    let mut value = queries::remote_part(&mut tx, upload, number).await?;
    value.read_lease = create_read_lease(&mut tx, value.stream.id, 120).await?;
    tx.commit().await?;
    Ok(Some(value))
}
pub(super) async fn finish_remote_part(
    pool: &PgPool,
    work: &RemotePart,
    etag: Option<&str>,
    node: crate::model::raw::Processor<'_>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let changed = queries::remote_part_finish(
        work,
        etag,
        node.context("native parts require a node")?.0.node_id,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(changed)
}
pub(super) async fn freeze_remote(
    pool: &PgPool,
    upload: Uuid,
    token: Uuid,
    hash: &str,
) -> Result<RemoteUpload> {
    let mut tx = pool.begin().await?;
    ensure!(
        queries::freeze_remote(upload, token, hash)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(value)
}
pub(super) async fn adopt_remote(pool: &PgPool, input: RemoteOutput<'_>) -> Result<Uuid> {
    ensure!(
        input.size >= 0 && input.digest.len() == 16 && input.hash.len() == 32,
        "invalid completed remote source"
    );
    let mut tx = pool.begin().await?;
    let source = queries::remote_output_lock(input.upload, input.token)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(crate::model::Error::OperationAborted)?;
    require_writing_stream(&mut tx, input.stream).await?;
    ensure!(
        queries::raw_prepare(input.stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::remote_source_finish(source, &input)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    queries::raw_attach(input.stream, source.into(), input.size)
        .execute(&mut *tx)
        .await?;
    if input.background {
        queries::raw_enqueue_processing(input.stream, source.into())
            .execute(&mut *tx)
            .await?;
        queries::remote_processing_hash(input.stream, input.hash)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(source)
}
pub(super) async fn claim_remote_cleanup(
    pool: &PgPool,
    token: Uuid,
    node: crate::model::raw::Processor<'_>,
) -> Result<Option<RemoteUpload>> {
    let mut tx = pool.begin().await?;
    raw_upload_processor(&mut tx, node).await?;
    let Some(upload) = queries::remote_cleanup_due()
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(None);
    };
    queries::remote_cleanup_claim(upload, token)
        .execute(&mut *tx)
        .await?;
    let value = queries::remote_upload(upload).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(value))
}

pub(super) async fn commit_conversion_batch(
    pool: &sqlx::PgPool,
    task: Execution,
    candidate: Id,
    rows: &[crate::model::Mapping],
    outputs: &[crate::model::packing::PackOutput],
) -> Result<()> {
    ensure!(
        !rows.is_empty() && rows.len() <= 1024,
        "invalid conversion batch"
    );
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    require_writing_stream(&mut tx, candidate).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let expected: std::collections::BTreeSet<_> = rows.iter().map(|r| r.chunk.id).collect();
    let produced: Vec<_> = outputs
        .iter()
        .flat_map(|o| o.chunks.iter().copied())
        .collect();
    ensure!(
        expected.len() == rows.len()
            && produced.len() == rows.len()
            && produced
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                == expected,
        "conversion outputs do not match inputs"
    );
    for row in rows {
        ensure!(
            queries::conversion_chunk(row.chunk.id, candidate)
                .fetch_optional(&mut *tx)
                .await?
                .is_some(),
            crate::model::Error::OperationAborted
        );
        queries::conversion_ready(row.chunk.id, candidate)
            .execute(&mut *tx)
            .await?;
        queries::conversion_retire_encoding(row.chunk.encoding_id)
            .execute(&mut *tx)
            .await?;
    }
    for output in outputs {
        if let Some(pack) = &output.pack {
            ensure!(
                output.independent.is_none()
                    && queries::conversion_output_pack(pack.id, task.id)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::publish_prepared_pack(pack.id)
                .execute(&mut *tx)
                .await?;
        } else if let Some(chunk) = &output.independent {
            ensure!(
                output.chunks == [chunk.id]
                    && queries::conversion_output_encoding(chunk.encoding_id, task.id, chunk.id)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some(),
                crate::model::Error::OperationAborted
            );
            queries::conversion_publish_encoding(chunk.encoding_id)
                .execute(&mut *tx)
                .await?;
            queries::publish_chunk_encoding(chunk)
                .execute(&mut *tx)
                .await?;
        } else {
            anyhow::bail!("conversion output has no physical source");
        }
        for chunk in &output.chunks {
            queries::conversion_layout(*chunk, output.pack.as_ref().map(|p| p.id))
                .execute(&mut *tx)
                .await?;
        }
    }
    for row in rows {
        queries::insert_chunk_extent(
            candidate,
            row.offset_bytes,
            row.chunk.raw_size,
            row.chunk.id,
        )
        .execute(&mut *tx)
        .await?;
    }
    require_sources(&mut tx, Some(candidate), &[]).await?;
    let end = rows.last().unwrap().offset_bytes + i64::from(rows.last().unwrap().chunk.raw_size);
    queries::resize_writing_stream(candidate, end)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn finish_conversion(
    pool: &sqlx::PgPool,
    task: Execution,
    job: &crate::model::raw::Conversion,
    source: &crate::model::raw::RawSource,
    candidate: Id,
    lease: Option<Uuid>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    lock_execution(&mut tx, task).await?;
    ensure!(
        queries::conversion_owned(candidate, task.id, task.token)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    if queries::conversion_current(job.stream, job.payload_revision)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        queries::conversion_abandon(candidate)
            .execute(&mut *tx)
            .await?;
        queries::complete_task(task.id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    }
    if let Some(lease) = lease {
        if queries::raw_processing_readers(job.stream, lease)
            .fetch_one(&mut *tx)
            .await?
        {
            return Ok(false);
        }
    }

    validate_raw(&mut tx, job.stream, source.size).await?;
    let current = queries::raw_source(source.id).fetch_one(&mut *tx).await?;
    ensure!(
        current == *source && source.id == job.source,
        crate::model::Error::SourceUnavailable
    );
    let (end, contiguous) = queries::extent_continuity(candidate)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        end == source.size && contiguous,
        "conversion candidate is incomplete"
    );
    if !queries::conversion_no_pending(candidate)
        .fetch_one(&mut *tx)
        .await?
    {
        return Ok(false);
    }
    require_sources(&mut tx, Some(candidate), &[]).await?;
    queries::conversion_move(candidate, job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_detach(job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_changed(job.stream)
        .execute(&mut *tx)
        .await?;
    queries::conversion_unpin(candidate)
        .execute(&mut *tx)
        .await?;
    queries::conversion_abandon(candidate)
        .execute(&mut *tx)
        .await?;
    queries::raw_touch_unreferenced(source.id)
        .execute(&mut *tx)
        .await?;

    queries::complete_task(task.id).execute(&mut *tx).await?;
    #[cfg(feature = "fault-injection")]
    crate::faults::point("cdc-switch-before-commit").await;
    tx.commit().await?;
    Ok(true)
}

pub(super) async fn inline_conversion(
    pool: &sqlx::PgPool,
    stream: Id,
    source: Id,
) -> Result<crate::model::cache::EncodingPolicy> {
    let mut tx = pool.begin().await?;
    require_writing_stream(&mut tx, stream).await?;
    ensure!(
        queries::raw_receiving(source, stream)
            .fetch_optional(&mut *tx)
            .await?
            .is_some(),
        crate::model::Error::OperationAborted
    );
    let job = queries::conversion_job(source).fetch_one(&mut *tx).await?.0;
    ensure!(
        queries::conversion_cancel(source)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::raw_discard(source, stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    ensure!(
        queries::conversion_inline(stream)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        crate::model::Error::OperationAborted
    );
    tx.commit().await?;
    Ok(job.encoding)
}

pub(super) async fn recover_chunk_memory(pool: &sqlx::PgPool, boot: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::release_memory_pins(boot).execute(&mut *tx).await?;
    queries::release_memory_chunks(boot)
        .execute(&mut *tx)
        .await?;
    queries::retire_memory_chunks(boot)
        .execute(&mut *tx)
        .await?;
    let lost = queries::quarantine_memory_chunks(boot)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    if lost != 0 {
        tracing::error!(
            lost,
            "volatile chunk sources were lost on restart and quarantined"
        );
    }
    Ok(())
}

pub(super) async fn recover_raw_memory(pool: &sqlx::PgPool, boot: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    queries::release_memory_raw(boot).execute(&mut *tx).await?;
    queries::release_memory_processing(boot)
        .execute(&mut *tx)
        .await?;
    let lost = queries::quarantine_memory_raw(boot)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    if lost != 0 {
        tracing::error!(
            lost,
            "volatile raw sources were lost on restart and quarantined"
        );
    }
    Ok(())
}
