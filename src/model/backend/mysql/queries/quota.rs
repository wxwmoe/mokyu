use super::*;

impl QuotaAccount {
    pub(in super::super) fn view(self, visible: bool) -> Quota {
        Quota {
            scope: self.scope,
            scope_id: self.scope_id,
            used_bytes: visible.then(|| self.used_bytes.to_string()),
            reserved_bytes: visible.then(|| self.reserved_bytes.to_string()),
            inflight_bytes: visible.then(|| self.inflight_bytes.to_string()),
            object_count: visible.then(|| self.object_count.to_string()),
            bucket_count: visible.then(|| self.bucket_count.to_string()),
            logical_bytes_limit: self.logical_bytes_limit.map(|n| n.to_string()),
            inflight_bytes_limit: self.inflight_bytes_limit.map(|n| n.to_string()),
            bucket_count_limit: self.bucket_count_limit.map(|n| n.to_string()),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct QuotaReservation {
    pub id: Id,
    pub bucket_id: Uuid,
    pub stream_id: Option<Id>,
    pub upload_id: Option<Uuid>,
    pub closed: bool,
    pub credit_bytes: i64,
    pub logical_bytes: i64,
    pub inflight_bytes: i64,
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct QuotaWrite {
    pub reservation_id: Id,
    pub allocated_bytes: i64,
    pub contribution_bytes: i64,
}

pub(in super::super) fn quota_account(scope: &str, id: Uuid, lock: bool) -> Rows<'_, QuotaAccount> {
    sqlx::query_as(if lock {
        "SELECT * FROM quota_scopes WHERE scope=? AND scope_id=? FOR UPDATE"
    } else {
        "SELECT * FROM quota_scopes WHERE scope=? AND scope_id=?"
    })
    .bind(scope)
    .bind(id)
}

pub(in super::super) fn insert_quota_scope(scope: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("INSERT INTO quota_scopes(scope,scope_id) VALUES(?,?)")
        .bind(scope)
        .bind(id)
}

pub(in super::super) fn delete_quota_scope(scope: &str, id: Uuid) -> Statement<'_> {
    sqlx::query("DELETE FROM quota_scopes WHERE scope=? AND scope_id=?")
        .bind(scope)
        .bind(id)
}

pub(in super::super) fn locked_project_quota(bucket: Uuid) -> Rows<'static, QuotaAccount> {
    sqlx::query_as("SELECT * FROM quota_scopes WHERE scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=? LOCK IN SHARE MODE) FOR UPDATE").bind(bucket)
}

pub(in super::super) fn quota_project(bucket: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT project_id FROM buckets WHERE id=? LOCK IN SHARE MODE").bind(bucket)
}

pub(in super::super) fn quota_membership(
    user: Option<Uuid>,
    project: Uuid,
) -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT scope FROM project_members WHERE user_id=? AND project_id=?")
        .bind(user)
        .bind(project)
}

pub(in super::super) fn update_quota(
    scope: &str,
    id: Uuid,
    bytes: Option<i64>,
    inflight: Option<i64>,
    buckets: Option<i64>,
) -> Statement<'_> {
    sqlx::query("UPDATE quota_scopes SET logical_bytes_limit=?,inflight_bytes_limit=?,bucket_count_limit=? WHERE scope=? AND scope_id=?").bind(bytes).bind(inflight).bind(buckets).bind(scope).bind(id)
}

pub(in super::super) fn adjust_quota(
    project: Uuid,
    bucket: Uuid,
    used: i64,
    reserved: i64,
    inflight: i64,
    objects: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_scopes SET used_bytes=used_bytes+?,reserved_bytes=reserved_bytes+?,inflight_bytes=inflight_bytes+?,object_count=object_count+? WHERE (scope='project' AND scope_id=?) OR (scope='bucket' AND scope_id=?)").bind(used).bind(reserved).bind(inflight).bind(objects).bind(project).bind(bucket)
}

pub(in super::super) fn owned_quota_reservation(
    stream: Id,
    upload: Option<Uuid>,
) -> Rows<'static, QuotaReservation> {
    match upload {
        Some(upload) => sqlx::query_as(concat!("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE upload_id=?", " FOR UPDATE")).bind(upload),
        None => sqlx::query_as(concat!("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE stream_id=?", " FOR UPDATE")).bind(stream),
    }
}

pub(in super::super) fn quota_reservation(id: Id) -> Rows<'static, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE id=?").bind(id)
}

pub(in super::super) fn publication_reservation(stream: Id) -> Rows<'static, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE stream_id=? OR result_stream_id=?").bind(stream).bind(stream)
}

pub(in super::super) fn superseded_reservations(
    bucket: Uuid,
    key: &str,
    stream: Id,
) -> Rows<'_, QuotaReservation> {
    sqlx::query_as("SELECT id,bucket_id,stream_id,upload_id,closed,credit_bytes,logical_bytes,inflight_bytes FROM quota_reservations WHERE bucket_id=? AND object_key=? AND stream_id<>? AND (logical_bytes>0 OR replaced_stream_id IS NOT NULL)").bind(bucket).bind(key.as_bytes()).bind(stream)
}

pub(in super::super) fn quota_write(stream: Id) -> Rows<'static, QuotaWrite> {
    sqlx::query_as("SELECT reservation_id,allocated_bytes,contribution_bytes FROM quota_writes WHERE stream_id=?").bind(stream)
}

pub(in super::super) fn quota_stream(stream: Id) -> Rows<'static, StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=? LOCK IN SHARE MODE").bind(stream)
}

pub(in super::super) fn quota_upload(
    upload: Uuid,
    completing: bool,
) -> Rows<'static, (Uuid, Vec<u8>)> {
    sqlx::query_as(if completing {
        "SELECT bucket_id,object_key FROM uploads WHERE id=? AND state='completing' FOR UPDATE"
    } else {
        "SELECT bucket_id,object_key FROM uploads WHERE id=? AND state='active' LOCK IN SHARE MODE"
    })
    .bind(upload)
}

pub(in super::super) fn object_claim(bucket: Uuid, key: &str, stream: Id) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=? AND object_key=? AND write_token=?)",
    )
    .bind(bucket)
    .bind(key.as_bytes())
    .bind(stream)
}

pub(in super::super) fn part_quota(upload: Uuid, part: i32, stream: Id) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT p.quota_size FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.upload_id=? AND p.part_number=? AND p.write_token=? AND u.state='active'").bind(upload).bind(part).bind(stream)
}

pub(in super::super) fn quota_credit(bucket: Uuid, key: &str) -> Rows<'_, (Id, i64)> {
    sqlx::query_as("SELECT o.stream_id,s.size FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=? AND o.object_key=? AND NOT EXISTS(SELECT 1 FROM quota_reservations r WHERE r.bucket_id=o.bucket_id AND r.object_key=o.object_key AND r.replaced_stream_id=o.stream_id)").bind(bucket).bind(key.as_bytes())
}

pub(in super::super) fn insert_quota_reservation(
    bucket: Uuid,
    key: &str,
    stream: Option<Id>,
    upload: Option<Uuid>,
    credit: Option<(Id, i64)>,
) -> Statement<'_> {
    sqlx::query("INSERT INTO quota_reservations(bucket_id,object_key,stream_id,upload_id,replaced_stream_id,credit_bytes) VALUES(?,?,?,?,?,?)").bind(bucket).bind(key.as_bytes()).bind(stream).bind(upload).bind(credit.map(|v|v.0)).bind(credit.map_or(0,|v|v.1))
}

pub(in super::super) fn insert_quota_write(stream: Id, owner: Id) -> Statement<'static> {
    sqlx::query("INSERT INTO quota_writes(stream_id,reservation_id) VALUES(?,?) ON DUPLICATE KEY UPDATE stream_id=quota_writes.stream_id").bind(stream).bind(owner)
}

pub(in super::super) fn change_quota_reservation(
    id: Id,
    logical: i64,
    inflight: i64,
    credit: i64,
) -> Statement<'static> {
    sqlx::query(
        "UPDATE quota_reservations SET logical_bytes=?,inflight_bytes=?,credit_bytes=? WHERE id=?",
    )
    .bind(logical)
    .bind(inflight)
    .bind(credit)
    .bind(id)
}

pub(in super::super) fn change_quota_write(
    stream: Id,
    allocated: i64,
    contribution: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE quota_writes SET allocated_bytes=?,contribution_bytes=? WHERE stream_id=?")
        .bind(allocated)
        .bind(contribution)
        .bind(stream)
}

pub(in super::super) fn clear_write_contributions(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=?").bind(owner)
}

pub(in super::super) fn clear_quota_credit(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET replaced_stream_id=NULL WHERE id=?").bind(owner)
}

pub(in super::super) fn close_quota_reservation(owner: Id) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET closed=TRUE,replaced_stream_id=NULL,result_stream_id=NULL WHERE id=?").bind(owner)
}

pub(in super::super) fn delete_quota_reservation(owner: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM quota_reservations WHERE id=?").bind(owner)
}

pub(in super::super) fn delete_quota_write(stream: Id) -> Statement<'static> {
    sqlx::query("DELETE FROM quota_writes WHERE stream_id=?").bind(stream)
}

pub(in super::super) fn pending_quota_writes(owner: Id) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(allocated_bytes),0) AS SIGNED) FROM quota_writes WHERE reservation_id=?").bind(owner)
}

pub(in super::super) fn remaining_quota_writes(owner: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM quota_writes WHERE reservation_id=?)")
        .bind(owner)
}

pub(in super::super) fn complete_quota(upload: Uuid, stream: Id, size: i64) -> Statement<'static> {
    sqlx::query("UPDATE quota_reservations SET result_stream_id=? WHERE upload_id=? AND logical_bytes>=? AND NOT closed").bind(stream).bind(upload).bind(size)
}

pub(in super::super) fn displaced_credits(
    bucket: Uuid,
    key: &str,
    old: Option<Id>,
) -> Scalar<'_, i64> {
    sqlx::query_scalar("SELECT CAST(COALESCE(SUM(LEAST(logical_bytes,credit_bytes)),0) AS SIGNED) FROM quota_reservations WHERE bucket_id=? AND object_key=? AND replaced_stream_id=?").bind(bucket).bind(key.as_bytes()).bind(old)
}

pub(in super::super) fn remove_displaced_credits(
    bucket: Uuid,
    key: &str,
    old: Option<Id>,
) -> Statement<'_> {
    sqlx::query("UPDATE quota_reservations SET credit_bytes=0,replaced_stream_id=NULL WHERE bucket_id=? AND object_key=? AND replaced_stream_id=?").bind(bucket).bind(key.as_bytes()).bind(old)
}

#[async_trait::async_trait]
impl QuotaStore for Backend {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
        transactions::read_quota(&self.pool, principal, scope, id).await
    }
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota> {
        transactions::save_quota(&self.pool, principal, scope, id, input).await
    }
    async fn reserve(
        &self,
        stream: Id,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64> {
        transactions::reserve_quota(&self.pool, stream, size, padding, upload).await
    }
    async fn complete(&self, upload: Uuid, stream: Id, size: i64) -> Result<()> {
        transactions::complete_quota(&self.pool, upload, stream, size).await
    }
}

pub(in super::super) fn write_authorization(
    id: Id,
) -> Scalar<'static, sqlx::types::Json<crate::authorization::Permit>> {
    sqlx::query_scalar("SELECT write_authorization FROM streams WHERE id=?").bind(id)
}

pub(in super::super) fn publish_stream_record(id: Id) -> Rows<'static, StoredStream> {
    sqlx::query_as("SELECT * FROM streams WHERE id=? FOR UPDATE").bind(id)
}

pub(in super::super) fn lock_object_version(bucket: Uuid, key: &str) -> Rows<'_, (Option<Id>, Id)> {
    sqlx::query_as(
        "SELECT stream_id,write_token FROM objects WHERE bucket_id=? AND object_key=? FOR UPDATE",
    )
    .bind(bucket)
    .bind(key.as_bytes())
}

pub(in super::super) fn web_upload_expectation(upload: Uuid) -> Rows<'static, (Option<Id>, i64)> {
    sqlx::query_as("SELECT expected_stream_id,expected_size FROM web_uploads WHERE upload_id=?")
        .bind(upload)
}

pub(in super::super) fn stream_size_etag(id: Id) -> Rows<'static, (i64, String)> {
    sqlx::query_as("SELECT size,etag FROM streams WHERE id=?").bind(id)
}

pub(in super::super) fn incomplete_extents(id: Id) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=? AND (e.fragment_id IS NOT NULL OR c.id IS NULL OR c.state<>'ready' OR e.source_offset+e.length>c.raw_size))").bind(id)
}

pub(in super::super) fn extent_continuity(id: Id) -> Rows<'static, (i64, bool)> {
    sqlx::query_as("SELECT COALESCE(MAX(offset_bytes+length),0),COALESCE(MIN(offset_bytes=previous_end),1)<>0 FROM (SELECT offset_bytes,length,COALESCE(MAX(offset_bytes+length) OVER(ORDER BY offset_bytes ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),0) AS previous_end FROM extents WHERE stream_id=?) e").bind(id)
}

pub(in super::super) fn catalog_category(key: &str, mime: &str) -> &'static str {
    let extension = key
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    if mime.starts_with("image/")
        || matches!(
            extension.as_str(),
            "png" | "jpg" | "jpeg" | "webp" | "gif" | "avif" | "svg" | "bmp" | "heic"
        )
    {
        "image"
    } else if mime.starts_with("video/")
        || matches!(
            extension.as_str(),
            "mp4" | "webm" | "mkv" | "mov" | "avi" | "m4v"
        )
    {
        "video"
    } else if mime.starts_with("audio/")
        || matches!(
            extension.as_str(),
            "mp3" | "ogg" | "opus" | "wav" | "flac" | "aac" | "m4a"
        )
    {
        "audio"
    } else if matches!(
        mime,
        "application/zip"
            | "application/gzip"
            | "application/x-7z-compressed"
            | "application/x-tar"
    ) || matches!(
        extension.as_str(),
        "zip" | "gz" | "7z" | "rar" | "tar" | "zst"
    ) {
        "archive"
    } else if mime.starts_with("text/")
        || mime == "application/pdf"
        || matches!(
            extension.as_str(),
            "pdf"
                | "txt"
                | "md"
                | "json"
                | "csv"
                | "htm"
                | "html"
                | "xml"
                | "doc"
                | "docx"
                | "xls"
                | "xlsx"
                | "ppt"
                | "pptx"
        )
    {
        "document"
    } else {
        "other"
    }
}
