use super::*;
use crate::model::journal::{BucketJournal, JournalStore};
use async_trait::async_trait;

pub(in super::super) fn lock_mode(bucket: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar(
        "SELECT CAST(journal_mode AS BIGINT) FROM bucket_change_gates WHERE bucket_id=?",
    )
    .bind(bucket)
}

pub(in super::super) fn enabled(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT g.journal_mode=1 FROM bucket_change_gates g JOIN buckets b ON b.id=g.bucket_id JOIN storages s ON s.id=b.storage_id WHERE b.id=? AND s.type='s3'").bind(bucket)
}

pub(in super::super) fn supported(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM buckets b JOIN storages s ON s.id=b.storage_id WHERE b.id=? AND s.type='s3')").bind(bucket)
}

pub(in super::super) fn create(bucket: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_buckets(bucket_id,storage_id,bucket_name,scan_until) SELECT b.id,b.storage_id,b.name,(SELECT o.object_key FROM objects o WHERE o.bucket_id=b.id ORDER BY o.object_key DESC LIMIT 1) FROM buckets b WHERE b.id=?").bind(bucket)
}

pub(in super::super) fn set_mode(bucket: Uuid, mode: i16) -> Statement<'static> {
    sqlx::query("UPDATE bucket_change_gates SET journal_mode=? WHERE bucket_id=?")
        .bind(mode)
        .bind(bucket)
}

pub(in super::super) fn restart_needed(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT dirty OR scanning FROM journal_buckets WHERE bucket_id=?")
        .bind(bucket)
}

pub(in super::super) fn restart(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET generation=generation+1,scan_after=NULL,scan_until=(SELECT o.object_key FROM objects o WHERE o.bucket_id=journal_buckets.bucket_id ORDER BY o.object_key DESC LIMIT 1),scanning=TRUE,dirty=FALSE WHERE bucket_id=?").bind(bucket)
}

pub(in super::super) fn advance(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=?").bind(bucket)
}

pub(in super::super) fn control<'a>(bucket: Uuid, operation: &'a str) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_entries(encrypt_frames,bucket_id,generation,seq,operation) SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=journal_buckets.storage_id),bucket_id,generation,last_seq,? FROM journal_buckets WHERE bucket_id=?").bind(operation).bind(bucket)
}

pub(in super::super) fn scanning(bucket: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT scanning FROM journal_buckets WHERE bucket_id=?").bind(bucket)
}

pub(in super::super) fn finish(bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET scanning=FALSE,scan_after=NULL,scan_until=NULL WHERE bucket_id=?")
        .bind(bucket)
}

pub(in super::super) fn seed(bucket: Uuid, limit: i64) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_entries(encrypt_frames,bucket_id,generation,seq,operation,object_key,stream_id,payload_revision,attributes) SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),p.bucket_id,j.generation,j.last_seq+ROW_NUMBER() OVER (ORDER BY p.object_key),'put',p.object_key,s.id,s.payload_revision,JSON_OBJECT('size',s.size,'etag',s.etag,'metadata',json(s.metadata),'checksums',json(s.checksums),'payload',s.payload_type,'modified_us',s.touched_at) FROM (SELECT o.bucket_id,o.object_key,o.stream_id FROM objects o JOIN journal_buckets j ON j.bucket_id=o.bucket_id WHERE o.bucket_id=? AND o.stream_id IS NOT NULL AND o.object_key<=j.scan_until AND (j.scan_after IS NULL OR o.object_key>j.scan_after) ORDER BY o.object_key LIMIT ?) p JOIN streams s ON s.id=p.stream_id JOIN journal_buckets j ON j.bucket_id=p.bucket_id").bind(bucket).bind(limit)
}

pub(in super::super) fn scanned(bucket: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET scan_after=(SELECT e.object_key FROM journal_entries e WHERE e.bucket_id=journal_buckets.bucket_id AND e.seq=journal_buckets.last_seq+?),last_seq=last_seq+? WHERE bucket_id=?").bind(count).bind(count).bind(bucket)
}

#[async_trait]
impl JournalStore for Backend {
    async fn bucket(&self, bucket: Uuid) -> Result<Option<BucketJournal>> {
        Ok(sqlx::query_as("SELECT CAST(generation AS TEXT) AS generation,CAST(last_seq AS TEXT) AS captured_sequence,CAST(published_seq AS TEXT) AS published_sequence,CAST(last_seq-published_seq AS TEXT) AS pending_records,scanning,dirty FROM journal_buckets WHERE bucket_id=?")
            .bind(bucket).fetch_optional(&self.read).await?)
    }
    async fn seed_page(&self, limit: i64) -> Result<bool> {
        let bucket: Option<Uuid> = sqlx::query_scalar("SELECT j.bucket_id FROM journal_buckets j JOIN bucket_change_gates g ON g.bucket_id=j.bucket_id WHERE j.scanning AND g.journal_mode=1 ORDER BY j.bucket_id LIMIT 1").fetch_optional(&self.write).await?;
        let Some(bucket) = bucket else {
            return Ok(false);
        };
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        let scanned = super::super::journal::seed(&mut tx, bucket, limit).await?;
        tx.commit().await?;
        Ok(scanned)
    }
}
