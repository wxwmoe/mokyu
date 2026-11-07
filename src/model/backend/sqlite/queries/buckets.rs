use super::*;

impl<'r> sqlx::FromRow<'r, SqliteRow> for ReceivedPart {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            number: row.try_get("number")?,
            size: row.try_get("size")?,
            etag: row.try_get("etag")?,
            sha256: row.try_get("sha256")?,
        })
    }
}

pub(in super::super) fn bucket_by_id(id: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=?").bind(id)
}

pub(in super::super) fn bucket_creation_policy(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT allow_bucket_create FROM projects WHERE id=?").bind(project)
}

pub(in super::super) fn bucket_creator_membership(
    project: Uuid,
    user: Option<Uuid>,
) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT role,scope FROM project_members WHERE project_id=? AND user_id=?")
        .bind(project)
        .bind(user)
}

pub(in super::super) fn bucket_available_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE enabled ORDER BY created_at,id")
}

pub(in super::super) fn insert_bucket(
    id: Uuid,
    name: &str,
    project: Uuid,
    storage: Uuid,
) -> Statement<'_> {
    sqlx::query("INSERT INTO buckets(id,name,project_id,storage_id) VALUES(?,?,?,?)")
        .bind(id)
        .bind(name)
        .bind(project)
        .bind(storage)
}

pub(in super::super) fn duplicate_bucket(name: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets WHERE name=?)").bind(name)
}

pub(in super::super) fn change_project_bucket_quota(
    id: Uuid,
    count: i64,
    bytes: i64,
    objects: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_scopes SET bucket_count=bucket_count+?,used_bytes=used_bytes+?,object_count=object_count+? WHERE scope='project' AND scope_id=?").bind(count).bind(bytes).bind(objects).bind(id)
}

pub(in super::super) fn bucket_has_streams(id: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=?) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=?)").bind(id).bind(id)
}

pub(in super::super) fn delete_bucket_objects(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=?").bind(id)
}

pub(in super::super) fn delete_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=?").bind(id)
}

pub(in super::super) fn bucket_member_ids(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM member_bucket_grants WHERE bucket_id=?) ORDER BY id").bind(id)
}

pub(in super::super) fn advance_bucket_credentials(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id IN (SELECT access_key FROM s3_credential_grants WHERE bucket_id=?)").bind(id)
}

pub(in super::super) fn advance_bucket_tokens(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE api_tokens SET auth_revision=auth_revision+1 WHERE id IN (SELECT token_id FROM token_bucket_grants WHERE bucket_id=?)").bind(id)
}

pub(in super::super) fn revoke_bucket_members(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE bucket_id=?").bind(id)
}

pub(in super::super) fn revoke_bucket_credentials(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE bucket_id=?").bind(id)
}

pub(in super::super) fn revoke_bucket_tokens(id: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE bucket_id=?").bind(id)
}

pub(in super::super) fn bucket_transfer_users(source: Uuid, target: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM project_members WHERE project_id IN (?,?)) ORDER BY id").bind(source).bind(target)
}

pub(in super::super) fn transfer_bucket(id: Uuid, project: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET project_id=?,uploads_paused=FALSE,settings_revision=settings_revision+1 WHERE id=?").bind(project).bind(id)
}

pub(in super::super) fn bucket_domain_owner(host: &str) -> Scalar<'_, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM bucket_domains WHERE host=?").bind(host)
}

pub(in super::super) fn insert_bucket_domain(host: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO bucket_domains(host,bucket_id) VALUES(?,?)")
        .bind(host)
        .bind(id)
}

pub(in super::super) fn delete_bucket_domain(host: &str) -> Statement<'_> {
    sqlx::query("DELETE FROM bucket_domains WHERE host=?").bind(host)
}

pub(in super::super) fn bucket_domain_names(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT host FROM bucket_domains WHERE bucket_id=? ORDER BY host").bind(id)
}

pub(in super::super) fn bucket_domains(id: Option<Uuid>) -> Rows<'static, Domain> {
    sqlx::query_as(
        "SELECT host,bucket_id FROM bucket_domains WHERE ? IS NULL OR bucket_id=? ORDER BY host",
    )
    .bind(id)
    .bind(id)
}

pub(in super::super) fn advance_bucket_settings(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET settings_revision=settings_revision+1 WHERE id=?").bind(id)
}

pub(in super::super) fn save_bucket_settings(id: Uuid, input: &BucketUpdate) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET cors=?,website_enabled=?,index_document=?,error_document=?,public_base_url=?,uploads_paused=?,settings_revision=settings_revision+1 WHERE id=?").bind(&input.cors).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(&input.public_base_url).bind(input.uploads_paused).bind(id)
}

pub(in super::super) fn set_bucket_cors(id: Uuid, rules: &Value) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET cors=?,settings_revision=settings_revision+1 WHERE id=? AND state='active'").bind(rules).bind(id)
}

pub(in super::super) fn set_bucket_website(id: Uuid, input: &Website) -> Statement<'_> {
    sqlx::query("UPDATE buckets SET website_enabled=?,index_document=?,error_document=?,settings_revision=settings_revision+1 WHERE id=? AND state='active'").bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(id)
}

pub(in super::super) fn bucket_impact(id: Uuid) -> Rows<'static, BucketImpact> {
    sqlx::query_as("SELECT b.id AS bucket_id,b.name,b.project_id,b.state,CAST(b.settings_revision AS TEXT) revision,b.uploads_paused,CAST(q.object_count AS TEXT) objects,CAST(q.used_bytes AS TEXT) logical_bytes,CAST(q.reserved_bytes AS TEXT) reserved_bytes,CAST(q.inflight_bytes AS TEXT) inflight_bytes,(SELECT CAST(COUNT(*) AS TEXT) FROM uploads WHERE bucket_id=b.id AND state IN ('active','completing')) active_uploads,(SELECT CAST(COUNT(*) AS TEXT) FROM streams WHERE bucket_id=b.id AND state='writing') writing_streams,(SELECT CAST(COUNT(*) AS TEXT) FROM member_bucket_grants WHERE bucket_id=b.id) member_bucket_grants,(SELECT CAST(COUNT(*) AS TEXT) FROM s3_credential_grants WHERE bucket_id=b.id) service_grants,(SELECT CAST(COUNT(*) AS TEXT) FROM token_bucket_grants WHERE bucket_id=b.id) token_bucket_grants,COALESCE((SELECT json_group_array(host) FROM bucket_domains WHERE bucket_id=b.id),JSON_ARRAY()) domains FROM buckets b JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id WHERE b.id=?").bind(id)
}

pub(in super::super) fn bucket_transfer_target(id: Uuid) -> Rows<'static, TransferProject> {
    sqlx::query_as("SELECT p.id,p.name,CAST(q.used_bytes AS TEXT) used_bytes,CAST(q.reserved_bytes AS TEXT) reserved_bytes,CAST(q.logical_bytes_limit AS TEXT) logical_bytes_limit,CAST(q.bucket_count AS TEXT) bucket_count,CAST(q.bucket_count_limit AS TEXT) bucket_count_limit,(SELECT CAST(COUNT(*) AS TEXT) FROM project_members WHERE project_id=p.id) members FROM projects p JOIN quota_scopes q ON q.scope='project' AND q.scope_id=p.id WHERE p.id=?").bind(id)
}

pub(in super::super) fn existing_purge(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='purge' AND bucket_id=? AND state<>'completed' LIMIT 1").bind(id)
}

pub(in super::super) fn seal_purge_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE buckets SET state='purging',settings_revision=settings_revision+1 WHERE id=?",
    )
    .bind(id)
}

pub(in super::super) fn insert_task(
    id: Uuid,
    task_type: &str,
    bucket: Option<Uuid>,
    detail: Value,
    audit: Option<Id>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO tasks(id,task_type,bucket_id,state,detail,created_by_label,source) VALUES(?,?,?,'queued',?,COALESCE((SELECT actor_label FROM audit_events WHERE id=?),'Scheduler'),COALESCE((SELECT source FROM audit_events WHERE id=?),'system'))").bind(id).bind(task_type).bind(bucket).bind(detail).bind(audit).bind(audit)
}

pub(in super::super) fn lock_task_execution(id: Execution) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=? AND state='running' AND execution_token=?")
        .bind(id.id)
        .bind(id.token)
}

pub(in super::super) fn lock_purge_objects(
    bucket: Uuid,
    cursor: Option<&str>,
    batch: i64,
) -> Rows<'_, (Vec<u8>, Option<Id>)> {
    sqlx::query_as("SELECT object_key,stream_id FROM objects WHERE bucket_id=? AND (? IS NULL OR object_key>?) ORDER BY object_key LIMIT ?").bind(bucket).bind(cursor.map(str::as_bytes)).bind(cursor.map(str::as_bytes)).bind(batch)
}

pub(in super::super) fn remove_object(bucket: Uuid, key: &str, token: Id) -> Statement<'_> {
    sqlx::query("UPDATE objects SET stream_id=NULL,write_token=?,catalog_size=NULL,catalog_modified=NULL,catalog_type=NULL,catalog_category=NULL,catalog_public=NULL WHERE bucket_id=? AND object_key=?").bind(token).bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn advance_task(id: Uuid, cursor: &str, processed: i64) -> Statement<'_> {
    sqlx::query("UPDATE tasks SET progress_cursor=?,processed=processed+?,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?").bind(cursor).bind(processed).bind(id)
}

pub(in super::super) fn complete_task(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}

pub(in super::super) async fn purge_upload_history(
    connection: &mut sqlx::SqliteConnection,
    bucket: Uuid,
    active: &[Uuid],
    batch: i64,
) -> Result<()> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "DELETE FROM uploads WHERE id IN (SELECT id FROM uploads WHERE bucket_id=",
    );
    query.push_bind(bucket).push(" AND state IN ('completed','aborted') AND NOT EXISTS(SELECT 1 FROM quota_reservations q WHERE q.upload_id=uploads.id)");
    if !active.is_empty() {
        query.push(" AND id NOT IN (");
        let mut values = query.separated(",");
        for id in active {
            values.push_bind(*id);
        }
        query.push(")");
    }
    query.push(" ORDER BY id LIMIT ").push_bind(batch).push(")");
    query.build().execute(connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl BucketStore for Backend {
    async fn domain_names(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT d.host,b.name FROM bucket_domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&self.read).await?)
    }
    async fn purge_counts(&self, bucket: Uuid) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT (SELECT COUNT(*) FROM objects WHERE bucket_id=? AND stream_id IS NOT NULL),(SELECT COUNT(*) FROM uploads WHERE bucket_id=? AND state IN ('active','completing'))").bind(bucket).bind(bucket).fetch_one(&self.read).await?)
    }
    async fn start_purge(
        &self,
        principal: &Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        transactions::start_purge(&self.write, principal, name, id, confirmation).await
    }
    async fn purge_uploads(&self, bucket: Uuid, default_batch: i32) -> Result<(i64, Vec<Uuid>)> {
        let batch: Option<i32>=sqlx::query_scalar("SELECT maintenance_concurrency FROM storage_profiles WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=?)").bind(bucket).fetch_one(&self.read).await?;
        let batch = i64::from(batch.unwrap_or(default_batch).max(1));
        Ok((batch,sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=? AND state IN ('active','completing') ORDER BY id LIMIT ?").bind(bucket).bind(batch).fetch_all(&self.read).await?))
    }
    async fn purge_batch(
        &self,
        task: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
        batch: i64,
        active: &[Uuid],
    ) -> Result<bool> {
        let _authorization = self.authorization.write().await;
        transactions::purge_batch(&self.write, task, bucket, cursor, batch, active).await
    }
    async fn by_name(&self, name: &str, active: bool) -> Result<Option<Bucket>> {
        Ok(
            sqlx::query_as("SELECT * FROM buckets WHERE name=? AND (NOT ? OR state='active')")
                .bind(name)
                .bind(active)
                .fetch_optional(&self.read)
                .await?,
        )
    }
    async fn by_host(&self, host: &str) -> Result<Option<Bucket>> {
        Ok(sqlx::query_as(
            "SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=?",
        )
        .bind(host)
        .fetch_optional(&self.read)
        .await?)
    }
    async fn all(&self) -> Result<Vec<Bucket>> {
        Ok(sqlx::query_as("SELECT * FROM buckets ORDER BY name")
            .fetch_all(&self.read)
            .await?)
    }
    async fn visible(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<BucketView>> {
        Ok(sqlx::query_as("SELECT b.*,CAST(IIF(?,127,IIF(? IS NOT NULL,COALESCE(t.actions,0),COALESCE(a.actions,0))) AS INTEGER) actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=? LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=? WHERE ? OR (IIF(? IS NOT NULL,t.actions,a.actions)&1)<>0 ORDER BY b.name LIMIT 1000").bind(admin).bind(token).bind(user).bind(token).bind(admin).bind(token).fetch_all(&self.read).await?)
    }
    async fn list_objects(
        &self,
        bucket: Uuid,
        prefix: &str,
        key: &str,
        inclusive: bool,
    ) -> Result<Vec<StoredStream>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=",
        );
        query
            .push_bind(bucket)
            .push(" AND o.object_key>=")
            .push_bind(prefix.as_bytes());
        if let Some(end) = crate::app::listing::successor(prefix) {
            query.push(" AND o.object_key<").push_bind(end.into_bytes());
        }
        query
            .push(" AND o.object_key")
            .push(if inclusive { ">=" } else { ">" })
            .push_bind(key.as_bytes())
            .push(" ORDER BY o.object_key LIMIT 64");
        Ok(query.build_query_as().fetch_all(&self.read).await?)
    }
    async fn create(&self, principal: &Principal, input: CreateBucket) -> Result<Bucket> {
        let _authorization = self.authorization.write().await;
        transactions::create_bucket(&self.write, principal, input).await
    }
    async fn delete(
        &self,
        principal: &Principal,
        bucket: Uuid,
        name: &str,
        confirmation: Option<&str>,
    ) -> Result<()> {
        let _authorization = self.authorization.write().await;
        transactions::delete_bucket(&self.write, principal, bucket, name, confirmation).await
    }
    async fn domains(&self, principal: &Principal, bucket: Option<Uuid>) -> Result<Vec<Domain>> {
        transactions::bucket_domains(&self.read, principal, bucket).await
    }
    async fn change_domain(
        &self,
        principal: &Principal,
        host: &str,
        bucket: Option<Uuid>,
    ) -> Result<()> {
        transactions::domain_change(&self.write, principal, host, bucket).await
    }
    async fn settings(&self, bucket: Uuid) -> Result<(Bucket, Vec<String>, Option<bool>)> {
        transactions::bucket_settings(&self.read, bucket).await
    }
    async fn save_settings(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: BucketUpdate,
    ) -> Result<()> {
        transactions::save_bucket_settings(&self.write, principal, bucket, input).await
    }
    async fn creation_options(
        &self,
        admin: bool,
        user: Uuid,
        session: bool,
    ) -> Result<(Vec<BucketProject>, Vec<BucketStorage>)> {
        Ok((sqlx::query_as("SELECT id,name FROM projects p WHERE ? OR (? AND allow_bucket_create AND EXISTS(SELECT 1 FROM project_members WHERE project_id=p.id AND user_id=? AND role='maintainer')) ORDER BY builtin DESC,name LIMIT 1000").bind(admin).bind(session).bind(user).fetch_all(&self.read).await?,sqlx::query_as("SELECT id,name,type AS storage_type FROM storages WHERE enabled ORDER BY name").fetch_all(&self.read).await?))
    }
    async fn impact(&self, principal: &Principal, bucket: Uuid) -> Result<BucketImpact> {
        transactions::preview_bucket(&self.read, principal, bucket).await
    }
    async fn transfer_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        target: Uuid,
    ) -> Result<(BucketImpact, TransferProject)> {
        transactions::preview_bucket_transfer(&self.read, principal, bucket, target).await
    }
    async fn transfer(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: TransferInput,
    ) -> Result<Bucket> {
        let _authorization = self.authorization.write().await;
        transactions::transfer_bucket(&self.write, principal, bucket, input).await
    }
    async fn cors(&self, bucket: Uuid) -> Result<Value> {
        Ok(sqlx::query_scalar("SELECT cors FROM buckets WHERE id=?")
            .bind(bucket)
            .fetch_optional(&self.read)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn set_cors(&self, principal: &Principal, bucket: Uuid, rules: Value) -> Result<Value> {
        transactions::set_bucket_cors(&self.write, principal, bucket, rules).await
    }
    async fn website(&self, bucket: Uuid) -> Result<Website> {
        Ok(sqlx::query_as(
            "SELECT website_enabled,index_document,error_document FROM buckets WHERE id=?",
        )
        .bind(bucket)
        .fetch_optional(&self.read)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn set_website(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: Website,
    ) -> Result<Website> {
        transactions::set_bucket_website(&self.write, principal, bucket, input).await
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketView {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        let mut result = BucketView::from(Bucket::from_row(row)?);
        result.actions = actions(row.try_get("actions")?)
            .into_iter()
            .map(|a| a.name().into())
            .collect();
        Ok(result)
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketProject {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketStorage {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            storage_type: row.try_get("storage_type")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for Domain {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            host: row.try_get("host")?,
            bucket_id: row.try_get("bucket_id")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for Website {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            website_enabled: row.try_get("website_enabled")?,
            index_document: row.try_get("index_document")?,
            error_document: row.try_get("error_document")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for TransferProject {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            used_bytes: row.try_get("used_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            logical_bytes_limit: row.try_get("logical_bytes_limit")?,
            bucket_count: row.try_get("bucket_count")?,
            bucket_count_limit: row.try_get("bucket_count_limit")?,
            members: row.try_get("members")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for BucketImpact {
    fn from_row(row: &'r SqliteRow) -> sqlx::Result<Self> {
        let sqlx::types::Json(mut domains): sqlx::types::Json<Vec<String>> =
            row.try_get("domains")?;
        domains.sort();
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            name: row.try_get("name")?,
            project_id: row.try_get("project_id")?,
            state: row.try_get("state")?,
            revision: row.try_get("revision")?,
            uploads_paused: row.try_get("uploads_paused")?,
            objects: row.try_get("objects")?,
            logical_bytes: row.try_get("logical_bytes")?,
            reserved_bytes: row.try_get("reserved_bytes")?,
            inflight_bytes: row.try_get("inflight_bytes")?,
            active_uploads: row.try_get("active_uploads")?,
            writing_streams: row.try_get("writing_streams")?,
            member_bucket_grants: row.try_get("member_bucket_grants")?,
            service_grants: row.try_get("service_grants")?,
            token_bucket_grants: row.try_get("token_bucket_grants")?,
            domains,
        })
    }
}
