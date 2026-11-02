use super::*;

pub(in super::super) fn publish_object(stream: &StoredStream) -> Statement<'_> {
    let mime = stream
        .metadata
        .get("content_type")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream");
    sqlx::query("UPDATE objects SET stream_id=?,catalog_size=?,catalog_modified=?,catalog_type=?,catalog_category=?,catalog_public=? WHERE bucket_id=? AND object_key=?").bind(stream.id).bind(stream.size).bind(stream.created_at).bind(mime).bind(catalog_category(&stream.object_key,mime)).bind(stream.public_read).bind(stream.bucket_id).bind(stream.object_key.as_bytes())
}

pub(in super::super) fn publish_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams s SET state='ready',touched_at=CURRENT_TIMESTAMP(6),http_compression=CASE WHEN stream_type='object' THEN (SELECT CASE WHEN COALESCE(sum(e.length),0)=s.size THEN JSON_OBJECT('estimated_size',CAST(COALESCE(sum(CASE WHEN e.source_offset=0 AND e.length=c.raw_size AND c.pack_id IS NULL AND COALESCE(l.standalone_size,c.standalone_size) IS NOT NULL THEN COALESCE(l.standalone_size,c.standalone_size) ELSE e.length END),0) AS SIGNED)) ELSE JSON_OBJECT() END FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE e.stream_id=s.id) ELSE JSON_OBJECT() END WHERE id=?")
        .bind(id)
}

pub(in super::super) fn compression_writable() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT NOT maintenance FROM mokyu_meta WHERE id=1 LOCK IN SHARE MODE")
}

pub(in super::super) fn learn_compression(
    observation: crate::model::CompressionObservation,
) -> Statement<'static> {
    sqlx::query("UPDATE streams SET http_compression=JSON_SET(CASE WHEN COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(http_compression,'$.version')) AS SIGNED),0)<? THEN CASE WHEN CAST(JSON_TYPE(JSON_EXTRACT(http_compression,'$.estimated_size')) AS BINARY)='INTEGER' AND CAST(JSON_UNQUOTE(JSON_EXTRACT(http_compression,'$.estimated_size')) AS SIGNED)>=0 THEN JSON_OBJECT('version',?,'estimated_size',CAST(JSON_UNQUOTE(JSON_EXTRACT(http_compression,'$.estimated_size')) AS SIGNED)) ELSE JSON_OBJECT('version',?) END ELSE http_compression END,?,?) WHERE id=? AND state='ready' AND COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(http_compression,'$.version')) AS SIGNED),0)<=? AND (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(http_compression,'$.version')) AS SIGNED),0)<? OR NOT JSON_CONTAINS_PATH(http_compression,'one',?))")
        .bind(observation.version).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding)).bind(observation.size).bind(observation.stream).bind(observation.version).bind(observation.version).bind(format!("$.{}",observation.encoding))
}

pub(in super::super) fn retire_stream(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE streams SET state='retired',touched_at=CURRENT_TIMESTAMP(6) WHERE id=?")
        .bind(id)
}

pub(in super::super) fn complete_upload<'a>(
    upload: Uuid,
    stream: Id,
    hash: &'a str,
    result: &'a Value,
    token: Uuid,
) -> Statement<'a> {
    sqlx::query("UPDATE uploads SET state='completed',result=?,output_stream=?,touched_at=CURRENT_TIMESTAMP(6) WHERE id=? AND state='completing' AND manifest_hash=? AND completion_token=? AND (completion_expires_at>CURRENT_TIMESTAMP(6) OR output_stream=?)").bind(result).bind(stream).bind(upload).bind(hash).bind(token).bind(stream)
}

pub(in super::super) fn upload_part_streams(upload: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT stream_id FROM parts WHERE upload_id=? AND stream_id IS NOT NULL ORDER BY part_number").bind(upload)
}

pub(in super::super) fn delete_upload_parts(upload: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM parts WHERE upload_id=?").bind(upload)
}

#[async_trait::async_trait]
impl PublishStore for Backend {
    async fn replicas_ready(&self, publication: Uuid) -> Result<bool> {
        let (phase, result) = super::super::queries::replica::state(publication)
            .fetch_one(&self.pool)
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
            .fetch_one(&self.pool)
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
