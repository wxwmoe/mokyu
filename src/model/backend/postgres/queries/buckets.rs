use super::*;

pub(in super::super) fn lock_bucket_id(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM buckets WHERE id=$1 FOR UPDATE").bind(bucket)
}

pub(in super::super) fn bucket_creation_policy(project: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT allow_bucket_create FROM projects WHERE id=$1 FOR SHARE")
        .bind(project)
}

pub(in super::super) fn bucket_creator_membership(
    project: Uuid,
    user: Option<Uuid>,
) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT role,scope FROM project_members WHERE project_id=$1 AND user_id=$2")
        .bind(project)
        .bind(user)
}

pub(in super::super) fn bucket_available_storages() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM storages WHERE enabled ORDER BY created_at,id")
}

pub(in super::super) fn insert_bucket<'q>(
    name: &'q str,
    project: Uuid,
    storage_id: Uuid,
) -> Rows<'q, Bucket> {
    sqlx::query_as("INSERT INTO buckets(name,project_id,storage_id) VALUES($1,$2,$3) ON CONFLICT(name) DO NOTHING RETURNING *").bind(name).bind(project).bind(storage_id)
}

pub(in super::super) fn grant_bucket_creator(
    user: Option<Uuid>,
    project: Uuid,
    bucket: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO member_bucket_grants(user_id,project_id,bucket_id,actions) VALUES($1,$2,$3,$4)").bind(user).bind(project).bind(bucket).bind(Action::ALL.map(|a| a.name()).to_vec())
}

pub(in super::super) fn lock_bucket_members(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM member_bucket_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket)
}

pub(in super::super) fn lock_bucket_credentials(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT access_key_id AS access_key FROM s3_credentials WHERE access_key_id IN (SELECT access_key FROM s3_credential_grants WHERE bucket_id=$1) ORDER BY access_key_id FOR UPDATE").bind(bucket)
}

pub(in super::super) fn lock_bucket_tokens(bucket: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM api_tokens WHERE id IN (SELECT token_id FROM token_bucket_grants WHERE bucket_id=$1) ORDER BY id FOR UPDATE").bind(bucket)
}

pub(in super::super) fn bucket_impact(bucket: Uuid) -> Rows<'static, BucketImpact> {
    sqlx::query_as("SELECT b.id AS bucket_id,b.name,b.project_id,b.state,b.settings_revision::text AS revision,b.uploads_paused,q.object_count::text AS objects,q.used_bytes::text AS logical_bytes,q.reserved_bytes::text,q.inflight_bytes::text,(SELECT count(*)::text FROM uploads WHERE bucket_id=b.id AND state IN ('active','completing')) AS active_uploads,(SELECT count(*)::text FROM streams WHERE bucket_id=b.id AND state='writing') AS writing_streams,(SELECT count(*)::text FROM member_bucket_grants WHERE bucket_id=b.id) AS member_bucket_grants,(SELECT count(*)::text FROM s3_credential_grants WHERE bucket_id=b.id) AS service_grants,(SELECT count(*)::text FROM token_bucket_grants WHERE bucket_id=b.id) AS token_bucket_grants,ARRAY(SELECT host FROM bucket_domains WHERE bucket_id=b.id ORDER BY host) AS domains FROM buckets b JOIN quota_scopes q ON q.scope='bucket' AND q.scope_id=b.id WHERE b.id=$1").bind(bucket)
}

pub(in super::super) fn lock_bucket(bucket: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=$1 FOR UPDATE").bind(bucket)
}

pub(in super::super) fn bucket_has_streams(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=$1) OR EXISTS(SELECT 1 FROM uploads WHERE bucket_id=$1)").bind(bucket)
}

pub(in super::super) fn delete_bucket_objects(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM objects WHERE bucket_id=$1").bind(bucket)
}

pub(in super::super) fn delete_bucket(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn bucket_domain_owner<'q>(value: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT bucket_id FROM bucket_domains WHERE host=$1").bind(value)
}

pub(in super::super) fn bucket_domain_state(id: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT state FROM buckets WHERE id=$1 FOR UPDATE").bind(id)
}

pub(in super::super) fn insert_bucket_domain<'q>(value: &'q str, id: Uuid) -> Statement<'q> {
    sqlx::query(
        "INSERT INTO bucket_domains(host,bucket_id) VALUES($1,$2) ON CONFLICT(host) DO NOTHING",
    )
    .bind(value)
    .bind(id)
}

pub(in super::super) fn delete_bucket_domain<'q>(value: &'q str) -> Statement<'q> {
    sqlx::query("DELETE FROM bucket_domains WHERE host=$1").bind(value)
}

pub(in super::super) fn bucket_domains(bucket: Option<Uuid>) -> Rows<'static, Domain> {
    sqlx::query_as("SELECT host,bucket_id FROM bucket_domains WHERE ($1::uuid IS NULL OR bucket_id=$1) ORDER BY host").bind(bucket)
}

pub(in super::super) fn bucket_snapshot() -> Statement<'static> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

pub(in super::super) fn bucket_by_id(bucket: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn bucket_domain_names(bucket: Uuid) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT host FROM bucket_domains WHERE bucket_id=$1 ORDER BY host")
        .bind(bucket)
}

pub(in super::super) fn bucket_creation_projects(
    admin: bool,
    user: Uuid,
    session: bool,
) -> Rows<'static, BucketProject> {
    sqlx::query_as("SELECT id,name FROM projects p WHERE $1 OR ($3 AND allow_bucket_create AND EXISTS(SELECT 1 FROM project_members WHERE project_id=p.id AND user_id=$2 AND role='maintainer')) ORDER BY builtin DESC,name LIMIT 1000").bind(admin).bind(user).bind(session)
}

pub(in super::super) fn bucket_creation_storages() -> Rows<'static, BucketStorage> {
    sqlx::query_as("SELECT id,name,type AS storage_type FROM storages WHERE enabled ORDER BY name")
}

pub(in super::super) fn bucket_domains_in_use<'q>(
    domains: &'q [String],
    bucket: Uuid,
) -> Scalar<'q, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM bucket_domains WHERE host=ANY($1) AND bucket_id<>$2)",
    )
    .bind(domains)
    .bind(bucket)
}

pub(in super::super) fn remove_bucket_domains<'q>(
    bucket: Uuid,
    domains: &'q [String],
) -> Statement<'q> {
    sqlx::query("DELETE FROM bucket_domains WHERE bucket_id=$1 AND NOT(host=ANY($2))")
        .bind(bucket)
        .bind(domains)
}

pub(in super::super) fn insert_bucket_domains<'q>(
    domains: &'q [String],
    bucket: Uuid,
) -> Statement<'q> {
    sqlx::query("INSERT INTO bucket_domains(host,bucket_id) SELECT unnest($1::text[]),$2 ON CONFLICT DO NOTHING").bind(domains).bind(bucket)
}

pub(in super::super) fn save_bucket_settings<'q>(
    bucket: Uuid,
    input: &'q BucketUpdate,
) -> Statement<'q> {
    sqlx::query("UPDATE buckets SET cors=$2,website_enabled=$3,index_document=$4,error_document=$5,public_base_url=$6,uploads_paused=$7 WHERE id=$1").bind(bucket).bind(&input.cors).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document).bind(&input.public_base_url).bind(input.uploads_paused)
}

pub(in super::super) fn bucket_transfer_target(id: Uuid) -> Rows<'static, TransferProject> {
    sqlx::query_as("SELECT p.id,p.name,q.used_bytes::text,q.reserved_bytes::text,q.logical_bytes_limit::text,q.bucket_count::text,q.bucket_count_limit::text,(SELECT count(*)::text FROM project_members WHERE project_id=p.id) AS members FROM projects p JOIN quota_scopes q ON q.scope='project' AND q.scope_id=p.id WHERE p.id=$1").bind(id)
}

pub(in super::super) fn bucket_project_id(bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn lock_bucket_transfer_users(
    source: Uuid,
    target: Uuid,
) -> Statement<'static> {
    sqlx::query("SELECT id FROM web_users WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1)) ORDER BY id FOR UPDATE").bind(vec![source, target])
}

pub(in super::super) fn revoke_bucket_members(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM member_bucket_grants WHERE bucket_id=$1").bind(bucket)
}

pub(in super::super) fn revoke_bucket_credentials(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM s3_credential_grants WHERE bucket_id=$1").bind(bucket)
}

pub(in super::super) fn revoke_bucket_tokens(bucket: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM token_bucket_grants WHERE bucket_id=$1").bind(bucket)
}

pub(in super::super) fn revoke_bucket_transfer_users(
    source: Uuid,
    target: Uuid,
) -> Statement<'static> {
    sqlx::query("UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id IN (SELECT user_id FROM project_members WHERE project_id=ANY($1))").bind(vec![source, target])
}

pub(in super::super) fn transfer_bucket(bucket: Uuid, target: Uuid) -> Rows<'static, Bucket> {
    sqlx::query_as("UPDATE buckets SET project_id=$2,uploads_paused=false WHERE id=$1 RETURNING *")
        .bind(bucket)
        .bind(target)
}

pub(in super::super) fn set_bucket_cors(bucket: Uuid, rules: Value) -> Scalar<'static, Value> {
    sqlx::query_scalar("UPDATE buckets SET cors=$2 WHERE id=$1 AND state='active' RETURNING cors")
        .bind(bucket)
        .bind(rules)
}

pub(in super::super) fn bucket_by_name<'q>(name: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE name=$1").bind(name)
}

pub(in super::super) fn list_bucket_objects<'q>(
    bucket: Uuid,
    prefix: &'q str,
    key: &'q str,
    inclusive: bool,
) -> Rows<'q, crate::app::StoredStream> {
    let pattern = format!(
        "{}%",
        prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    sqlx::query_as("SELECT s.* FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=$1 AND o.object_key LIKE $2 AND (o.object_key>$3 OR ($4 AND o.object_key=$3)) ORDER BY o.object_key LIMIT 64").bind(bucket).bind(pattern).bind(key).bind(inclusive)
}

pub(in super::super) fn active_bucket_by_name<'q>(name: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as("SELECT * FROM buckets WHERE name=$1 AND state='active'").bind(name)
}

pub(in super::super) fn bucket_by_host<'q>(host: &'q str) -> Rows<'q, Bucket> {
    sqlx::query_as(
        "SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=$1",
    )
    .bind(host)
}

pub(in super::super) fn visible_buckets(
    user: Uuid,
    admin: bool,
    token: Option<Uuid>,
) -> Rows<'static, BucketView> {
    sqlx::query_as("SELECT b.*,b.settings_revision::text AS revision,CASE WHEN $2 THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect'] WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END AS actions FROM buckets b LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$1 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$3 WHERE $2 OR 'bucket.list'=ANY(CASE WHEN $3::uuid IS NOT NULL THEN t.actions ELSE a.actions END) ORDER BY b.name LIMIT 1000").bind(user).bind(admin).bind(token)
}

pub(in super::super) fn bucket_cors(bucket: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar("SELECT cors FROM buckets WHERE id=$1").bind(bucket)
}

pub(in super::super) fn bucket_website(bucket: Uuid) -> Rows<'static, Website> {
    sqlx::query_as("SELECT website_enabled,index_document,error_document FROM buckets WHERE id=$1")
        .bind(bucket)
}

pub(in super::super) fn set_bucket_website<'q>(
    bucket: Uuid,
    input: &'q Website,
) -> Rows<'q, Website> {
    sqlx::query_as("UPDATE buckets SET website_enabled=$2,index_document=$3,error_document=$4 WHERE id=$1 AND state='active' RETURNING website_enabled,index_document,error_document").bind(bucket).bind(input.website_enabled).bind(&input.index_document).bind(&input.error_document)
}

#[async_trait::async_trait]
impl BucketStore for Backend {
    async fn domain_names(&self) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT d.host,b.name FROM bucket_domains d JOIN buckets b ON b.id=d.bucket_id ORDER BY d.host").fetch_all(&self.pool).await?)
    }

    async fn purge_counts(&self, bucket: Uuid) -> Result<(i64, i64)> {
        Ok(purge_counts(bucket).fetch_one(&self.pool).await?)
    }
    async fn start_purge(
        &self,
        principal: &Principal,
        name: &str,
        id: Uuid,
        confirmation: Option<&str>,
    ) -> Result<Value> {
        transactions::start_purge(&self.pool, principal, name, id, confirmation).await
    }
    async fn purge_uploads(&self, bucket: Uuid, default_batch: i32) -> Result<(i64, Vec<Uuid>)> {
        let batch = purge_batch_limit(bucket)
            .fetch_one(&self.pool)
            .await?
            .unwrap_or(default_batch)
            .max(1) as i64;
        Ok((
            batch,
            purge_uploads(bucket, batch).fetch_all(&self.pool).await?,
        ))
    }
    async fn purge_batch(
        &self,
        task: Execution,
        bucket: Uuid,
        cursor: Option<&str>,
        batch: i64,
        active: &[Uuid],
    ) -> Result<bool> {
        transactions::purge_batch(&self.pool, task, bucket, cursor, batch, active).await
    }

    async fn by_name(&self, name: &str, active: bool) -> Result<Option<Bucket>> {
        Ok(if active {
            active_bucket_by_name(name)
                .fetch_optional(&self.pool)
                .await?
        } else {
            bucket_by_name(name).fetch_optional(&self.pool).await?
        })
    }
    async fn by_host(&self, host: &str) -> Result<Option<Bucket>> {
        Ok(bucket_by_host(host).fetch_optional(&self.pool).await?)
    }
    async fn all(&self) -> Result<Vec<Bucket>> {
        Ok(sqlx::query_as("SELECT * FROM buckets ORDER BY name")
            .fetch_all(&self.pool)
            .await?)
    }
    async fn visible(
        &self,
        user: Uuid,
        admin: bool,
        token: Option<Uuid>,
    ) -> Result<Vec<BucketView>> {
        Ok(visible_buckets(user, admin, token)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn list_objects(
        &self,
        bucket: Uuid,
        prefix: &str,
        key: &str,
        inclusive: bool,
    ) -> Result<Vec<crate::app::StoredStream>> {
        Ok(list_bucket_objects(bucket, prefix, key, inclusive)
            .fetch_all(&self.pool)
            .await?)
    }
    async fn creation_options(
        &self,
        admin: bool,
        user: Uuid,
        session: bool,
    ) -> Result<(Vec<BucketProject>, Vec<BucketStorage>)> {
        Ok((
            bucket_creation_projects(admin, user, session)
                .fetch_all(&self.pool)
                .await?,
            bucket_creation_storages().fetch_all(&self.pool).await?,
        ))
    }
    async fn cors(&self, bucket: Uuid) -> Result<Value> {
        Ok(bucket_cors(bucket)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn website(&self, bucket: Uuid) -> Result<Website> {
        Ok(bucket_website(bucket)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchBucket))?)
    }
    async fn create(&self, principal: &Principal, input: CreateBucket) -> Result<Bucket> {
        transactions::create_bucket(&self.pool, principal, input).await
    }
    async fn delete(
        &self,
        principal: &Principal,
        bucket: Uuid,
        name: &str,
        confirmation: Option<&str>,
    ) -> Result<()> {
        transactions::delete_bucket(&self.pool, principal, bucket, name, confirmation).await
    }
    async fn domains(&self, principal: &Principal, bucket: Option<Uuid>) -> Result<Vec<Domain>> {
        transactions::bucket_domains(&self.pool, principal, bucket).await
    }
    async fn change_domain(
        &self,
        principal: &Principal,
        host: &str,
        bucket: Option<Uuid>,
    ) -> Result<()> {
        transactions::domain_change(&self.pool, principal, host, bucket).await
    }
    async fn settings(&self, bucket: Uuid) -> Result<(Bucket, Vec<String>)> {
        transactions::bucket_settings(&self.pool, bucket).await
    }
    async fn save_settings(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: BucketUpdate,
    ) -> Result<()> {
        transactions::save_bucket_settings(&self.pool, principal, bucket, input).await
    }
    async fn impact(&self, principal: &Principal, bucket: Uuid) -> Result<BucketImpact> {
        transactions::preview_bucket(&self.pool, principal, bucket).await
    }
    async fn transfer_preview(
        &self,
        principal: &Principal,
        bucket: Uuid,
        target: Uuid,
    ) -> Result<(BucketImpact, TransferProject)> {
        transactions::preview_bucket_transfer(&self.pool, principal, bucket, target).await
    }
    async fn transfer(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: TransferInput,
    ) -> Result<Bucket> {
        transactions::transfer_bucket(&self.pool, principal, bucket, input).await
    }
    async fn set_cors(&self, principal: &Principal, bucket: Uuid, rules: Value) -> Result<Value> {
        transactions::set_bucket_cors(&self.pool, principal, bucket, rules).await
    }
    async fn set_website(
        &self,
        principal: &Principal,
        bucket: Uuid,
        input: Website,
    ) -> Result<Website> {
        transactions::set_bucket_website(&self.pool, principal, bucket, input).await
    }
}
