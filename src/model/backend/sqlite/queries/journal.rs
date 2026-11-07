pub(in super::super) use super::read_grants::node_read_stream as stream;
use super::*;
use crate::model::journal::publication::{Allocation, File, Member, Page, Part};
use crate::model::journal::{
    BucketJournal, Cursor, Draft, Entry, JournalStore, Ledger, Status, policy::Policy,
};
use crate::{
    config::Secrets,
    storage::{journal::Format, marker::Journal},
};
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

pub(in super::super) fn ensure_store(storage: Uuid) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_stores(policy,storage_id) SELECT ?,id FROM storages WHERE id=? AND type='s3' ON CONFLICT DO NOTHING").bind(sqlx::types::Json(Policy::default())).bind(storage)
}

pub(in super::super) fn claim(storage: Uuid, token: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET lease_token=?,lease_until=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE storage_id=? AND (lease_token IS NULL OR lease_until<=CAST(unixepoch('subsec')*1000000 AS INTEGER) OR lease_token=?)").bind(token).bind(storage).bind(token)
}

pub(in super::super) fn fence(storage: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT storage_id FROM journal_stores WHERE storage_id=? AND lease_token=? AND lease_until>CAST(unixepoch('subsec')*1000000 AS INTEGER)").bind(storage).bind(token)
}

pub(in super::super) fn ledger(storage: Uuid) -> Rows<'static, Ledger> {
    sqlx::query_as("SELECT storage_id,policy,revision,descriptor,marker_installed,allocated_seq,allocated_digest,emitted_seq,open_file,flush_requested,last_bucket,CAST(unixepoch('subsec')*1000000 AS INTEGER) AS now_us FROM journal_stores WHERE storage_id=?").bind(storage)
}

pub(in super::super) fn event(bucket: Uuid) -> Rows<'static, Entry> {
    sqlx::query_as("SELECT e.encrypt_frames,e.bucket_id,j.bucket_name,e.seq,e.generation,e.operation,CAST(e.object_key AS TEXT) AS object_key,e.stream_id,e.attributes FROM journal_entries e JOIN journal_buckets j ON j.bucket_id=e.bucket_id WHERE e.bucket_id=? AND e.seq>j.published_seq AND NOT EXISTS(SELECT 1 FROM journal_drafts d WHERE d.bucket_id=e.bucket_id AND d.seq=e.seq AND d.final_page) ORDER BY e.seq LIMIT 1").bind(bucket)
}

pub(in super::super) fn cursor(bucket: Uuid, seq: i64) -> Rows<'static, Cursor> {
    sqlx::query_as("SELECT page,end_offset,range_count,range_digest,source_revision,final_page FROM journal_drafts WHERE bucket_id=? AND seq=? ORDER BY page DESC LIMIT 1").bind(bucket).bind(seq)
}

pub(in super::super) fn clear_drafts(bucket: Uuid, seq: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_drafts WHERE bucket_id=? AND seq=?")
        .bind(bucket)
        .bind(seq)
}

pub(in super::super) fn clear_holds(bucket: Uuid, seq: i64) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_holds WHERE bucket_id=? AND seq=?")
        .bind(bucket)
        .bind(seq)
}

pub(in super::super) fn key_material(
    id: Uuid,
) -> Rows<'static, (String, String, Vec<u8>, Vec<u8>)> {
    sqlx::query_as("SELECT name,key_algorithm,secret_encrypted,fingerprint FROM managed_keys WHERE id=? AND purpose='block'").bind(id)
}

pub(in super::super) fn hold<'a>(
    bucket: Uuid,
    seq: i64,
    hold: &'a crate::model::journal::record::Hold,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_holds(bucket_id,seq,storage_id,unit,address,block_key) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING").bind(bucket).bind(seq).bind(hold.storage).bind(hold.unit).bind(hold.address).bind(hold.key)
}

pub(in super::super) fn save_draft<'a>(
    bucket: Uuid,
    seq: i64,
    state: &'a Cursor,
    data: &'a [u8],
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_drafts(bucket_id,seq,page,end_offset,range_count,range_digest,source_revision,final_page,encrypted) VALUES(?,?,?,?,?,?,?,?,?)").bind(bucket).bind(seq).bind(state.page).bind(state.end_offset).bind(state.range_count).bind(&state.range_digest).bind(state.source_revision).bind(state.final_page).bind(data)
}

pub(in super::super) fn rotate(storage: Uuid, bucket: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET last_bucket=? WHERE storage_id=?")
        .bind(bucket)
        .bind(storage)
}

pub(in super::super) fn file(storage: Uuid, id: Uuid) -> Rows<'static, File> {
    sqlx::query_as("SELECT f.encrypt_frames,f.id,f.storage_id,f.epoch,f.first_seq,f.last_seq,f.previous_digest,f.digest,f.byte_count,f.mode,f.state,f.part_cut,f.part_since_us,f.opened_at AS opened_us,f.oldest_at AS oldest_us,f.upload_opened_at AS upload_opened_us,f.upload_id FROM journal_files f WHERE f.storage_id=? AND f.id=?").bind(storage).bind(id)
}

pub(in super::super) fn insert_file(f: &File) -> Statement<'_> {
    sqlx::query("INSERT INTO journal_files(id,storage_id,epoch,first_seq,last_seq,previous_digest,digest,byte_count,mode,state,part_cut,part_since_us,opened_at,oldest_at,encrypt_frames) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(f.id).bind(f.storage_id).bind(f.epoch).bind(f.first_seq).bind(f.last_seq).bind(&f.previous_digest).bind(&f.digest).bind(f.byte_count).bind(&f.mode).bind(&f.state).bind(f.part_cut).bind(f.part_since_us).bind(f.opened_us).bind(f.oldest_us).bind(f.encrypt_frames)
}

pub(in super::super) fn update_file(f: &File) -> Statement<'_> {
    sqlx::query("UPDATE journal_files SET last_seq=?,digest=?,byte_count=?,state=?,oldest_at=?,part_cut=?,part_since_us=?,encrypt_frames=? WHERE id=? AND state='open'").bind(f.last_seq).bind(&f.digest).bind(f.byte_count).bind(&f.state).bind(f.oldest_us).bind(f.part_cut).bind(f.part_since_us).bind(f.encrypt_frames).bind(f.id)
}

pub(in super::super) fn frame<'a>(id: Uuid, offset: i64, bytes: &'a [u8]) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_file_frames(file_id,byte_offset,encoded) VALUES(?,?,?)")
        .bind(id)
        .bind(offset)
        .bind(bytes)
}

pub(in super::super) fn member(m: &Member) -> Statement<'static> {
    sqlx::query("INSERT INTO journal_file_entries(file_id,bucket_id,seq,byte_offset,encoded_bytes,page,final_page) VALUES(?,?,?,?,?,?,?)").bind(m.file).bind(m.page.bucket).bind(m.page.seq).bind(m.offset).bind(m.bytes).bind(m.page.number).bind(m.final_page)
}

pub(in super::super) fn assign(page: Page) -> Statement<'static> {
    sqlx::query("UPDATE journal_drafts SET assigned=TRUE WHERE bucket_id=? AND seq=? AND page=? AND NOT assigned").bind(page.bucket).bind(page.seq).bind(page.number)
}

pub(in super::super) fn dictionary<'a>(
    storage: Uuid,
    epoch: Uuid,
    id: &'a [u8],
    hash: &'a [u8],
    file: Uuid,
) -> Statement<'a> {
    sqlx::query("INSERT INTO journal_dictionary(storage_id,epoch,identity_hash,content_hash,first_file) VALUES(?,?,?,?,?)").bind(storage).bind(epoch).bind(id).bind(hash).bind(file)
}

pub(in super::super) fn allocated(storage: Uuid, plan: &Allocation) -> Statement<'_> {
    sqlx::query("UPDATE journal_stores SET allocated_seq=?,allocated_digest=?,open_file=? WHERE storage_id=?").bind(plan.sequence).bind(plan.digest.as_slice()).bind(plan.open).bind(storage)
}

pub(in super::super) fn part_row(id: Uuid, num: i32) -> Rows<'static, Part> {
    sqlx::query_as("SELECT file_id,number,byte_offset,byte_count,digest,etag FROM journal_parts WHERE file_id=? AND number=?").bind(id).bind(num)
}

pub(in super::super) fn insert_part(p: &Part) -> Statement<'_> {
    sqlx::query("INSERT INTO journal_parts(file_id,number,byte_offset,byte_count,digest,etag) VALUES(?,?,?,?,?,?)").bind(p.file_id).bind(p.number).bind(p.byte_offset).bind(p.byte_count).bind(&p.digest).bind(&p.etag)
}

pub(in super::super) fn part_etag(p: &Part) -> Statement<'_> {
    sqlx::query("UPDATE journal_parts SET etag=? WHERE file_id=? AND number=? AND etag IS NULL")
        .bind(&p.etag)
        .bind(p.file_id)
        .bind(p.number)
}

pub(in super::super) fn part_members(p: &Part) -> Statement<'static> {
    sqlx::query("UPDATE journal_file_entries SET part_number=? WHERE file_id=? AND byte_offset>=? AND byte_offset<?").bind(p.number).bind(p.file_id).bind(p.byte_offset).bind(p.byte_offset+p.byte_count)
}

pub(in super::super) fn published<'a>(
    id: Uuid,
    etag: Option<&'a str>,
    version: Option<&'a str>,
) -> Statement<'a> {
    sqlx::query("UPDATE journal_files SET state='published',etag=?,object_version=?,confirmed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE id=? AND state='sealed'").bind(etag).bind(version).bind(id)
}

pub(in super::super) fn emitted(f: &File) -> Statement<'_> {
    sqlx::query("UPDATE journal_stores SET emitted_seq=?,seal_digest=?,confirmed_at=CAST(unixepoch('subsec')*1000000 AS INTEGER),last_error=NULL WHERE storage_id=?").bind(f.last_seq).bind(&f.digest).bind(f.storage_id)
}

pub(in super::super) fn watermarks(file: Uuid) -> Rows<'static, (Uuid, i64, Option<i64>, i64)> {
    sqlx::query_as("SELECT m.bucket_id,MAX(m.seq),MAX(CASE WHEN e.operation='ready' THEN e.generation END),j.published_seq FROM journal_file_entries m JOIN journal_entries e ON e.bucket_id=m.bucket_id AND e.seq=m.seq JOIN journal_buckets j ON j.bucket_id=m.bucket_id WHERE m.file_id=? AND m.final_page GROUP BY m.bucket_id,j.published_seq").bind(file)
}

pub(in super::super) fn watermark(
    bucket: Uuid,
    seq: i64,
    generation: Option<i64>,
) -> Statement<'static> {
    sqlx::query("UPDATE journal_buckets SET prepared_generation=COALESCE(?,prepared_generation),published_seq=? WHERE bucket_id=? AND published_seq<?").bind(generation).bind(seq).bind(bucket).bind(seq)
}

pub(in super::super) fn release_streams(
    bucket: Uuid,
    previous: i64,
    seq: i64,
) -> Statement<'static> {
    sqlx::query("UPDATE journal_entries SET stream_id=NULL WHERE bucket_id=? AND seq>? AND seq<=? AND stream_id IS NOT NULL").bind(bucket).bind(previous).bind(seq)
}

pub(in super::super) fn drop_frames(file: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM journal_file_frames WHERE file_id=?").bind(file)
}

pub(in super::super) fn apply_policy(storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET policy=pending_policy,pending_policy=NULL WHERE storage_id=? AND open_file IS NULL AND allocated_seq=emitted_seq AND pending_policy IS NOT NULL").bind(storage)
}

pub(in super::super) fn finish_flush(storage: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE journal_stores SET flush_requested=FALSE WHERE storage_id=? AND flush_requested AND NOT EXISTS(SELECT 1 FROM journal_buckets j WHERE j.storage_id=journal_stores.storage_id AND j.flush_seq>j.published_seq)").bind(storage)
}

#[async_trait]
impl JournalStore for Backend {
    async fn marker_intents(&self) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT storage_id FROM journal_stores WHERE descriptor IS NOT NULL AND NOT marker_installed").fetch_all(&self.read).await?)
    }
    async fn installed(
        &self,
        storage: Uuid,
        token: Uuid,
        previous: &serde_json::Value,
        updated: &serde_json::Value,
    ) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        let actual: sqlx::types::Json<serde_json::Value> =
            sqlx::query_scalar("SELECT identity_marker FROM storages WHERE id=?")
                .bind(storage)
                .fetch_one(&mut *tx)
                .await?;
        anyhow::ensure!(
            actual.0 == *previous || actual.0 == *updated,
            "storage marker changed during journal activation"
        );
        if actual.0 != *updated {
            super::bump_storage_profile(storage)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE storages SET identity_marker=? WHERE id=?")
            .bind(sqlx::types::Json(updated))
            .bind(storage)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE journal_stores SET marker_installed=TRUE WHERE storage_id=?")
            .bind(storage)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn sources(&self) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT storage_id FROM journal_buckets WHERE last_seq>published_seq UNION SELECT storage_id FROM journal_stores WHERE open_file IS NOT NULL OR allocated_seq>emitted_seq OR flush_requested ORDER BY storage_id").fetch_all(&self.read).await?)
    }

    async fn claim(&self, storage: Uuid, token: Uuid) -> Result<Option<Ledger>> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        ensure_store(storage).execute(&mut *tx).await?;
        let changed = claim(storage, token)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let ledger = if changed == 1 {
            Some(ledger(storage).fetch_one(&mut *tx).await?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(ledger)
    }

    async fn renew(&self, storage: Uuid, token: Uuid) -> Result<bool> {
        Ok(sqlx::query("UPDATE journal_stores SET lease_until=CAST(unixepoch('subsec')*1000000 AS INTEGER)+60000000 WHERE storage_id=? AND lease_token=? AND lease_until>CAST(unixepoch('subsec')*1000000 AS INTEGER)")
            .bind(storage).bind(token).execute(&self.write).await?.rows_affected()==1)
    }

    async fn release(&self, storage: Uuid, token: Uuid, error: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE journal_stores SET lease_token=NULL,lease_until=NULL,last_error=? WHERE storage_id=? AND lease_token=?")
            .bind(error).bind(storage).bind(token).execute(&self.write).await?;
        Ok(())
    }

    async fn descriptor(&self, storage: Uuid, token: Uuid, journal: &Journal) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        let changed = sqlx::query(
            "UPDATE journal_stores SET descriptor=? WHERE storage_id=? AND descriptor IS NULL",
        )
        .bind(sqlx::types::Json(journal))
        .bind(storage)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        anyhow::ensure!(changed == 1, "journal descriptor already claimed");
        tx.commit().await?;
        Ok(())
    }

    async fn due_buckets(
        &self,
        storage: Uuid,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT bucket_id FROM journal_buckets WHERE storage_id=? AND published_seq<last_seq ORDER BY CASE WHEN bucket_id>? THEN 0 ELSE 1 END,bucket_id LIMIT ?")
            .bind(storage).bind(after.unwrap_or(Uuid::nil())).bind(limit).fetch_all(&self.read).await?)
    }

    async fn prepare_error(
        &self,
        storage: Uuid,
        token: Uuid,
        bucket: Uuid,
        error: &str,
    ) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        sqlx::query(
            "UPDATE journal_buckets SET prepare_error=? WHERE bucket_id=? AND storage_id=?",
        )
        .bind(error)
        .bind(bucket)
        .bind(storage)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn prepare_page(
        &self,
        storage: Uuid,
        bucket: Uuid,
        token: Uuid,
        format: &Format<'_>,
        secrets: &Secrets,
    ) -> Result<bool> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        let progress =
            super::super::journal::prepare(&mut tx, storage, bucket, token, format, secrets)
                .await?;
        if progress {
            sqlx::query("UPDATE journal_buckets SET prepare_error=NULL WHERE bucket_id=? AND prepare_error IS NOT NULL").bind(bucket).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(progress)
    }

    async fn drafts(&self, bucket: Uuid, limit: i64) -> Result<Vec<Draft>> {
        Ok(sqlx::query_as("SELECT d.bucket_id,d.seq,d.page,d.encrypted,d.final_page,done.ready_at AS recorded_us FROM journal_drafts d JOIN journal_drafts done ON done.bucket_id=d.bucket_id AND done.seq=d.seq AND done.final_page JOIN journal_buckets j ON j.bucket_id=d.bucket_id WHERE d.bucket_id=? AND NOT d.assigned ORDER BY d.bucket_id,d.seq,d.page LIMIT ?")
           .bind(bucket).bind(limit).fetch_all(&self.read).await?)
    }

    async fn open(&self, storage: Uuid, id: Uuid) -> Result<File> {
        Ok(file(storage, id).fetch_one(&self.read).await?)
    }

    async fn next_file(&self, storage: Uuid) -> Result<Option<File>> {
        Ok(sqlx::query_as("SELECT f.id,f.storage_id,f.epoch,f.first_seq,f.last_seq,f.previous_digest,f.digest,f.byte_count,f.mode,f.state,f.part_cut,f.part_since_us,f.opened_at AS opened_us,f.oldest_at AS oldest_us,f.upload_opened_at AS upload_opened_us,f.upload_id FROM journal_files f JOIN journal_stores j ON j.storage_id=f.storage_id WHERE f.storage_id=? AND f.state='sealed' AND f.first_seq=j.emitted_seq+1 ORDER BY f.first_seq LIMIT 1")
            .bind(storage).fetch_optional(&self.read).await?)
    }

    async fn known(
        &self,
        storage: Uuid,
        epoch: Uuid,
        ids: &[[u8; 32]],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut q = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT identity_hash,content_hash FROM journal_dictionary WHERE storage_id=",
        );
        q.push_bind(storage)
            .push(" AND epoch=")
            .push_bind(epoch)
            .push(" AND identity_hash IN (");
        let mut items = q.separated(",");
        for id in ids {
            items.push_bind(id.as_slice());
        }
        q.push(")");
        Ok(q.build_query_as().fetch_all(&self.read).await?)
    }

    async fn allocate(&self, storage: Uuid, token: Uuid, plan: &Allocation) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        super::super::journal::allocate(&mut tx, storage, token, plan).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn file_frames(&self, file: Uuid) -> Result<Vec<Vec<u8>>> {
        Ok(sqlx::query_scalar(
            "SELECT encoded FROM journal_file_frames WHERE file_id=? ORDER BY byte_offset",
        )
        .bind(file)
        .fetch_all(&self.read)
        .await?)
    }
    async fn start_upload(&self, storage: Uuid, file: Uuid, token: Uuid) -> Result<bool> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        let started=sqlx::query("UPDATE journal_files SET upload_opened_at=CAST(unixepoch('subsec')*1000000 AS INTEGER) WHERE storage_id=? AND id=? AND upload_opened_at IS NULL AND state IN ('open','sealed')")
            .bind(storage).bind(file).execute(&mut *tx).await?.rows_affected()==1;
        tx.commit().await?;
        Ok(started)
    }

    async fn upload_id(&self, storage: Uuid, file: Uuid, token: Uuid, upload: &str) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        let changed=sqlx::query("UPDATE journal_files SET upload_id=?,upload_opened_at=COALESCE(upload_opened_at,CAST(unixepoch('subsec')*1000000 AS INTEGER)) WHERE id=? AND storage_id=? AND upload_id IS NULL AND state IN ('open','sealed')")
            .bind(upload).bind(file).bind(storage).execute(&mut *tx).await?.rows_affected();
        anyhow::ensure!(changed == 1, "journal upload already claimed");
        tx.commit().await?;
        Ok(())
    }
    async fn parts(&self, file: Uuid) -> Result<Vec<Part>> {
        Ok(sqlx::query_as("SELECT file_id,number,byte_offset,byte_count,digest,etag FROM journal_parts WHERE file_id=? ORDER BY number").bind(file).fetch_all(&self.read).await?)
    }
    async fn part(&self, storage: Uuid, token: Uuid, part: &Part) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        super::super::journal::part(&mut tx, storage, token, part).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn acknowledge(
        &self,
        storage: Uuid,
        token: Uuid,
        file: &File,
        etag: Option<&str>,
        version: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        super::super::journal::acknowledge(&mut tx, storage, token, file, etag, version).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn idle(&self, storage: Uuid, token: Uuid) -> Result<()> {
        let mut tx = self.write.begin_with(BEGIN_WRITE).await?;
        fence(storage, token).fetch_one(&mut *tx).await?;
        apply_policy(storage).execute(&mut *tx).await?;
        finish_flush(storage).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn journal_status(&self, storage: Uuid) -> Result<Status> {
        let supported: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM storages WHERE id=? AND type='s3')")
                .bind(storage)
                .fetch_one(&self.read)
                .await?;
        anyhow::ensure!(
            supported,
            s3s::s3_error!(
                InvalidArgument,
                "metadata journal requires a managed S3 storage"
            )
        );
        let row: Option<(sqlx::types::Json<Policy>,bool,i64,i64,Option<Uuid>,Option<i64>,Option<String>)> =
            sqlx::query_as("SELECT COALESCE(pending_policy,policy),pending_policy IS NOT NULL,revision,emitted_seq,open_file,confirmed_at,last_error FROM journal_stores WHERE storage_id=?")
            .bind(storage).fetch_optional(&self.read).await?;
        let (policy, pending_policy, revision, seq, open_file, confirmed, error) = row
            .unwrap_or_else(|| {
                (
                    sqlx::types::Json(Policy::default()),
                    false,
                    1,
                    0,
                    None,
                    None,
                    None,
                )
            });
        Ok(Status {
            policy: policy.0,
            pending_policy,
            revision: revision.to_string(),
            emitted_sequence: seq.to_string(),
            open_file,
            confirmed_at: confirmed.and_then(chrono::DateTime::from_timestamp_micros),
            error,
        })
    }

    async fn policy(
        &self,
        principal: &Principal,
        storage: Uuid,
        revision: i64,
        policy: &Policy,
    ) -> Result<()> {
        policy.validate()?;
        let mut tx = transactions::admin_transaction(&self.write, principal).await?;
        ensure_store(storage).execute(&mut *tx).await?;
        let changed = sqlx::query("UPDATE journal_stores SET policy=CASE WHEN open_file IS NULL AND allocated_seq=emitted_seq THEN ? ELSE policy END,pending_policy=CASE WHEN open_file IS NULL AND allocated_seq=emitted_seq THEN NULL ELSE ? END,flush_requested=CASE WHEN open_file IS NOT NULL OR allocated_seq>emitted_seq THEN TRUE ELSE flush_requested END,revision=revision+1 WHERE storage_id=? AND revision=?")
            .bind(sqlx::types::Json(policy)).bind(sqlx::types::Json(policy)).bind(storage).bind(revision).execute(&mut *tx).await?.rows_affected();
        anyhow::ensure!(
            changed == 1,
            s3s::s3_error!(PreconditionFailed, "journal policy revision changed")
        );
        tx.commit().await?;
        Ok(())
    }

    async fn flush(&self, principal: &Principal, storage: Uuid) -> Result<()> {
        let mut tx = transactions::admin_transaction(&self.write, principal).await?;
        ensure_store(storage).execute(&mut *tx).await?;
        sqlx::query("UPDATE journal_buckets SET flush_seq=last_seq WHERE storage_id=?")
            .bind(storage)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE journal_stores SET flush_requested=TRUE WHERE storage_id=? AND EXISTS(SELECT 1 FROM journal_buckets b WHERE b.storage_id=journal_stores.storage_id AND b.last_seq>b.published_seq)").bind(storage).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn bucket(&self, bucket: Uuid) -> Result<Option<BucketJournal>> {
        Ok(sqlx::query_as("SELECT CAST(generation AS TEXT) AS generation,CAST(last_seq AS TEXT) AS captured_sequence,CAST(published_seq AS TEXT) AS published_sequence,CAST(last_seq-published_seq AS TEXT) AS pending_records,scanning,dirty,prepared_generation=generation AS baseline_ready,prepare_error AS error FROM journal_buckets WHERE bucket_id=?")
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
