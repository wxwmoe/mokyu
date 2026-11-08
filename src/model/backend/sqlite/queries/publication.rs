use super::*;

pub(in super::super) fn publish_object(stream: &StoredStream) -> Statement<'_> {
    let mime = stream
        .metadata
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream");
    sqlx::query("UPDATE objects SET stream_id=?,catalog_size=?,catalog_modified=?,catalog_type=?,catalog_category=?,catalog_public=? WHERE bucket_id=? AND object_key=?").bind(stream.id).bind(stream.size).bind(stream.created_at.timestamp_micros()).bind(mime).bind(catalog_category(&stream.object_key,mime)).bind(stream.public_read).bind(stream.bucket_id).bind(stream.object_key.as_bytes())
}

pub(in super::super) fn publish_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams AS s SET state='ready',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),http_compression=CASE WHEN stream_type='object' THEN (SELECT CASE WHEN COALESCE(sum(e.length),0)=s.size THEN json_object('estimated_size',COALESCE(sum(CASE WHEN e.source_offset=0 AND e.length=c.raw_size AND c.pack_id IS NULL AND COALESCE(l.standalone_size,c.standalone_size) IS NOT NULL THEN COALESCE(l.standalone_size,c.standalone_size) ELSE e.length END),0)) ELSE '{}' END FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE e.stream_id=s.id) ELSE '{}' END WHERE id=?")
        .bind(id)
}

pub(in super::super) fn learn_compression(
    observation: crate::model::CompressionObservation,
) -> Statement<'static> {
    sqlx::query("UPDATE streams SET http_compression=json_set(CASE WHEN COALESCE(json_extract(http_compression,'$.version'),0)<? THEN CASE WHEN json_type(http_compression,'$.estimated_size')='integer' AND json_extract(http_compression,'$.estimated_size')>=0 THEN json_object('version',?,'estimated_size',json_extract(http_compression,'$.estimated_size')) ELSE json_object('version',?) END ELSE http_compression END,?,?) WHERE id=? AND state='ready' AND COALESCE(json_extract(http_compression,'$.version'),0)<=? AND (COALESCE(json_extract(http_compression,'$.version'),0)<? OR json_type(http_compression,?) IS NULL) AND EXISTS(SELECT 1 FROM mokyu_meta WHERE id=1 AND NOT maintenance)")
        .bind(observation.version).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding)).bind(observation.size).bind(observation.stream).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding))
}

pub(in super::super) fn retire_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=?")
        .bind(id)
}

pub(in super::super) fn complete_upload<'a>(
    upload: Uuid,
    stream: Id,
    hash: &'a str,
    result: &'a Value,
    token: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completed',result=?,output_stream=?,touched_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='completing' AND manifest_hash=? AND completion_token=? AND (completion_expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER) OR output_stream=?)").bind(result).bind(stream).bind(upload).bind(hash).bind(token).bind(stream)
}

pub(in super::super) fn upload_part_streams(upload: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT stream_id FROM parts WHERE upload_id=? AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(in super::super) fn delete_upload_parts(upload: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM parts WHERE upload_id=?").bind(upload)
}

pub(in super::super) async fn ready_stream_queues(
    connection: &mut sqlx::SqliteConnection,
    stream: Id,
) -> Result<()> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id) SELECT id FROM streams WHERE id=? AND state='ready' AND stream_type='object' AND EXISTS(SELECT 1 FROM extents WHERE stream_id=? AND chunk_id IS NOT NULL LIMIT 1 OFFSET 1) ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,progress_cursor=0,reason='pack',updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),next_check_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(stream).bind(stream).execute(&mut *connection).await?;
    Ok(())
}

#[async_trait::async_trait]
impl QuotaStore for Backend {
    async fn read(&self, principal: &Principal, scope: &str, id: Uuid) -> Result<Quota> {
        transactions::read_quota(&self.read, principal, scope, id).await
    }
    async fn save(
        &self,
        principal: &Principal,
        scope: &str,
        id: Uuid,
        input: Limits,
    ) -> Result<Quota> {
        transactions::save_quota(&self.write, principal, scope, id, input).await
    }
    async fn reserve(
        &self,
        stream: Id,
        size: i64,
        padding: bool,
        upload: Option<(Uuid, i32)>,
    ) -> Result<i64> {
        transactions::reserve_quota(&self.write, stream, size, padding, upload).await
    }
    async fn complete(&self, upload: Uuid, stream: Id, size: i64) -> Result<()> {
        transactions::complete_quota(&self.write, upload, stream, size).await
    }
}

#[async_trait::async_trait]
impl PublishStore for Backend {
    async fn replicas_ready(&self, publication: Uuid) -> Result<bool> {
        let (phase, result) = super::super::queries::replica::state(publication)
            .fetch_one(&self.read)
            .await?;
        if phase == "published" {
            return Ok(true);
        }
        if phase == "aborted" {
            return Err(crate::model::publish::reject_replica(
                result.as_ref().and_then(|r| r.0["error"].as_str()),
            ));
        }
        let (_, failed) = super::super::queries::replica::readiness(publication)
            .fetch_one(&self.read)
            .await?;
        anyhow::ensure!(!failed, crate::model::Error::OperationAborted);
        Ok(false)
    }

    async fn publish(
        &self,
        stream: Id,
        if_match: Option<s3s::dto::ETagCondition>,
        if_none: Option<s3s::dto::ETagCondition>,
        completion: Option<(Uuid, String, Value, Uuid)>,
    ) -> Result<bool> {
        transactions::publish(self, stream, if_match, if_none, completion).await
    }
}
